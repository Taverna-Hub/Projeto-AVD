from contextlib import asynccontextmanager
import os
import threading
import logging

from fastapi import FastAPI

from .routers import upload, status, processed
from .services.mlflow_monitor import router as mlflow_sync_router, mlflow_monitor
from .services.mlflow_service import mlflow_service
from .services.thingsboard_service import thingsboard_service

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Inicializar serviços
    logger.info("Inicializando serviços...")
    mlflow_service.initialize()
    
    # Autenticar no ThingsBoard
    tb_username = os.getenv("TB_USERNAME", "tenant@thingsboard.org")
    tb_password = os.getenv("TB_PASSWORD", "tenant")
    thingsboard_service.authenticate(tb_username, tb_password)
    
    # Iniciar MLflow Monitor em background se habilitado
    enable_monitor = os.getenv("ENABLE_MLFLOW_MONITOR", "false").lower() == "true"
    monitor_thread = None
    
    if enable_monitor:
        logger.info("Iniciando MLflow Monitor em background...")
        mlflow_tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
        polling_interval = int(os.getenv("MLFLOW_POLLING_INTERVAL", "30"))
        experiments = os.getenv("MLFLOW_MONITORED_EXPERIMENTS", "Imputacao_por_Estacao").split(",")
        
        mlflow_monitor.mlflow_tracking_uri = mlflow_tracking_uri
        mlflow_monitor.check_interval = polling_interval
        
        # Configurar S3
        mlflow_monitor.s3_bucket_name = os.getenv("S3_BUCKET_NAME")
        mlflow_monitor.s3_prefix = os.getenv("S3_PREFIX", "dados_imputados")
        
        if mlflow_monitor.s3_bucket_name:
            try:
                import boto3
                mlflow_monitor.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                    region_name=os.getenv("AWS_REGION", "us-east-1")
                )
                logger.info(f"S3 configurado: bucket={mlflow_monitor.s3_bucket_name}, prefix={mlflow_monitor.s3_prefix}")
            except Exception as e:
                logger.error(f"Erro ao configurar S3: {e}")
        else:
            logger.warning("S3 não configurado - defina S3_BUCKET_NAME no .env")
        
        if mlflow_monitor.initialize():
            monitor_thread = threading.Thread(
                target=mlflow_monitor.start_monitoring,
                args=(experiments, True),
                daemon=True
            )
            monitor_thread.start()
            logger.info(f"MLflow Monitor iniciado (intervalo: {polling_interval}s, experimentos: {experiments})")
        else:
            logger.error("Falha ao inicializar MLflow Monitor")
    else:
        logger.info("MLflow Monitor desabilitado (use ENABLE_MLFLOW_MONITOR=true para ativar)")
    
    logger.info("Serviços inicializados com sucesso")
    
    yield
    
    # Shutdown: parar monitor se estiver rodando
    if monitor_thread and monitor_thread.is_alive():
        logger.info("Parando MLflow Monitor...")
        mlflow_monitor.stop_monitoring()
        monitor_thread.join(timeout=5)


app = FastAPI(
    title="Data to S3 Upload Service",
    description="API para ler arquivos da pasta data e fazer upload para S3",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(upload.router, prefix="/api/v1", tags=["upload"])
app.include_router(status.router, prefix="/api/v1", tags=["status"])
app.include_router(processed.router, prefix="/api/v1", tags=["processed"])
app.include_router(mlflow_sync_router, prefix="/api/v1", tags=["mlflow-sync"])

@app.get("/")
async def root():
    return {
        "message": "Data to S3 Upload Service",
        "version": "1.0.0",
        "endpoints": {
            "upload_all": "/api/v1/upload/all",
            "upload_year": "/api/v1/upload/year/{year}",
            "status": "/api/v1/status",
            "processed_pipeline": "/api/v1/processed/pipeline",
            "processed_health": "/api/v1/processed/health",
            "mlflow_sync_start": "/api/v1/mlflow-sync/start",
            "mlflow_sync_status": "/api/v1/mlflow-sync/status",
            "mlflow_sync_now": "/api/v1/mlflow-sync/sync-now",
        },
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}
