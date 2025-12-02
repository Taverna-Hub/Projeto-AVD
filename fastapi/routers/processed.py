"""
Router para processamento de dados tratados e integração com MLflow/ThingsBoard/Trendz.
"""

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
import logging

from ..services.processed_pipeline import create_pipeline
from ..config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/processed", tags=["processed-data"])


class ProcessDataRequest(BaseModel):
    """Request para processar dados do notebook."""
    results_pkl_path: str = Field(
        ...,
        description="Caminho para o arquivo results.pkl gerado pelo notebook",
        example="../notebooks/results.pkl"
    )
    station_name: str = Field(
        default="A307_PETROLINA",
        description="Nome da estação meteorológica"
    )
    export_to_thingsboard: bool = Field(
        default=True,
        description="Se deve exportar dados para ThingsBoard"
    )
    export_to_trendz: bool = Field(
        default=True,
        description="Se deve sincronizar com Trendz Analytics"
    )
    device_token: Optional[str] = Field(
        default=None,
        description="Token do dispositivo no ThingsBoard"
    )
    tb_username: Optional[str] = Field(
        default=None,
        description="Usuário do ThingsBoard para autenticação"
    )
    tb_password: Optional[str] = Field(
        default=None,
        description="Senha do ThingsBoard para autenticação"
    )


class ProcessDataResponse(BaseModel):
    """Response do processamento de dados."""
    success: bool
    message: str
    station_name: Optional[str] = None
    records_processed: Optional[int] = None
    duration_seconds: Optional[float] = None
    s3_upload: Optional[Dict[str, Any]] = None
    mlflow_run: Optional[Dict[str, Any]] = None
    thingsboard_export: Optional[Dict[str, Any]] = None
    trendz_sync: Optional[Dict[str, Any]] = None
    metrics: Optional[Dict[str, float]] = None
    error: Optional[str] = None


@router.post(
    "/pipeline",
    response_model=ProcessDataResponse,
    status_code=status.HTTP_200_OK,
    summary="Processa dados tratados e distribui para S3/MLflow/ThingsBoard/Trendz",
    description="""
    Executa pipeline completo de dados tratados:
    1. Carrega results.pkl do notebook
    2. Cria DataFrame processado com predições
    3. Upload para S3 (pasta processed-data)
    4. Registra experimento no MLflow
    5. (Opcional) Envia telemetria para ThingsBoard
    6. (Opcional) Sincroniza com Trendz Analytics
    """
)
async def process_and_export(request: ProcessDataRequest) -> ProcessDataResponse:
    """
    Processa dados tratados do notebook e exporta para múltiplos destinos.
    """
    try:
        # Criar pipeline
        pipeline = create_pipeline(
            bucket_name=settings.S3_BUCKET_NAME,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            aws_region=settings.AWS_REGION,
            s3_prefix=settings.S3_PREFIX,
            mlflow_uri=settings.MLFLOW_TRACKING_URI
        )
        
        # Autenticar ThingsBoard/Trendz se credenciais fornecidas
        if request.export_to_thingsboard and request.tb_username and request.tb_password:
            pipeline.tb_service.authenticate(request.tb_username, request.tb_password)
        
        if request.export_to_trendz and request.tb_username and request.tb_password:
            pipeline.trendz_service.authenticate(request.tb_username, request.tb_password)
        
        # Executar pipeline
        result = pipeline.process_and_export_notebook_results(
            results_pkl_path=request.results_pkl_path,
            station_name=request.station_name,
            export_to_tb=request.export_to_thingsboard,
            export_to_trendz=request.export_to_trendz,
            device_token=request.device_token
        )
        
        if result["success"]:
            return ProcessDataResponse(
                success=True,
                message="Dados processados e exportados com sucesso",
                **result
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=result.get("error", "Erro desconhecido no pipeline")
            )
    
    except FileNotFoundError as e:
        logger.error(f"Arquivo não encontrado: {e}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Arquivo results.pkl não encontrado: {str(e)}"
        )
    
    except Exception as e:
        logger.error(f"Erro ao processar dados: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao processar dados: {str(e)}"
        )


@router.get(
    "/health",
    summary="Verifica status dos serviços integrados",
    description="Verifica conectividade com S3, MLflow, ThingsBoard e Trendz"
)
async def health_check() -> Dict[str, Any]:
    """
    Verifica status de todos os serviços integrados.
    """
    try:
        pipeline = create_pipeline(
            bucket_name=settings.S3_BUCKET_NAME,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            aws_region=settings.AWS_REGION,
            s3_prefix=settings.S3_PREFIX,
            mlflow_uri=settings.MLFLOW_TRACKING_URI
        )
        
        # Verificar S3
        s3_healthy = pipeline.s3_service.check_bucket_exists()
        
        # Verificar MLflow
        mlflow_healthy = pipeline.mlflow_service._initialized
        
        return {
            "status": "healthy" if (s3_healthy and mlflow_healthy) else "degraded",
            "services": {
                "s3": {
                    "status": "up" if s3_healthy else "down",
                    "bucket": settings.S3_BUCKET_NAME
                },
                "mlflow": {
                    "status": "up" if mlflow_healthy else "down",
                    "tracking_uri": settings.MLFLOW_TRACKING_URI
                },
                "thingsboard": {
                    "status": "unknown",
                    "note": "Requer autenticação para verificar"
                },
                "trendz": {
                    "status": "unknown",
                    "note": "Requer autenticação para verificar"
                }
            }
        }
    
    except Exception as e:
        logger.error(f"Erro no health check: {e}")
        return {
            "status": "unhealthy",
            "error": str(e)
        }
