"""
Router para gerenciar devices e telemetria das estações meteorológicas.
"""

import logging
from typing import Dict, List, Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends
from pydantic import BaseModel

from ..config import settings
from ..services.thingsboard_service import thingsboard_service
from ..services.device_manager_service import (
    create_device_manager,
    ESTACOES_METEOROLOGICAS
)
from ..services.csv_processor_service import create_csv_processor
from ..services.graph_metadata_service import create_graph_metadata_service
from ..services.s3_service import create_s3_service

logger = logging.getLogger(__name__)

router = APIRouter()


# Modelos de resposta
class DeviceInfo(BaseModel):
    """Informações de um device."""
    nome: str
    device_id: str
    token: str
    

class DeviceCreationResponse(BaseModel):
    """Resposta da criação de devices."""
    sucesso: int
    falhas: int
    devices: List[DeviceInfo]
    

class TelemetrySendStatus(BaseModel):
    """Status do envio de telemetria."""
    status: str
    message: str
    task_id: Optional[str] = None
    

class TelemetryResult(BaseModel):
    """Resultado do envio de telemetria."""
    estacao: str
    arquivos_processados: int
    registros_enviados: int
    sucesso: int
    falhas: int
    taxa_sucesso: float


class MetadataResponse(BaseModel):
    """Resposta com metadados de gráficos."""
    estacao: str
    total_arquivos: int
    modelos_disponiveis: List[str] = []


# Estado global para tracking de tarefas
telemetry_tasks: Dict[str, Dict] = {}


def get_device_manager():
    """Dependency para obter DeviceManager."""
    return create_device_manager(thingsboard_service)


def get_csv_processor():
    """Dependency para obter CSVProcessor."""
    data_dir = str(settings.get_data_directory())
    return create_csv_processor(data_dir, thingsboard_service)


def get_metadata_service():
    """Dependency para obter GraphMetadataService."""
    notebooks_dir = str(settings.get_data_directory().parent / "notebooks")
    return create_graph_metadata_service(notebooks_dir)


def get_s3_service():
    """Dependency para obter S3Service."""
    return create_s3_service(
        bucket_name=settings.S3_BUCKET_NAME,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        aws_region=settings.AWS_REGION,
        s3_prefix=settings.S3_PREFIX
    )


@router.get("/devices/estacoes")
async def listar_estacoes():
    """
    Lista todas as estações meteorológicas disponíveis.
    
    Returns:
        Lista de estações com seus códigos e descrições
    """
    return {
        "total": len(ESTACOES_METEOROLOGICAS),
        "estacoes": ESTACOES_METEOROLOGICAS
    }


@router.post("/devices/setup", response_model=DeviceCreationResponse)
async def criar_devices(
    incluir_metadados: bool = True,
    device_manager=Depends(get_device_manager),
    metadata_service=Depends(get_metadata_service)
):
    """
    Cria todos os devices para as estações meteorológicas.
    
    Args:
        incluir_metadados: Se deve incluir metadados de gráficos como atributos
        
    Returns:
        Resultado da criação dos devices
    """
    logger.info("Iniciando criação de devices para estações meteorológicas")
    
    # Extrair metadados se solicitado
    metadados_estacoes = None
    if incluir_metadados:
        try:
            logger.info("Extraindo metadados de gráficos...")
            metadados_todas = metadata_service.extrair_metadados_todas_estacoes()
            
            # Formatar para atributos
            metadados_estacoes = {}
            for estacao, metadados in metadados_todas.items():
                metadados_estacoes[estacao] = metadata_service.formatar_para_atributos_device(metadados)
            
            logger.info(f"Metadados extraídos para {len(metadados_estacoes)} estações")
        except Exception as e:
            logger.error(f"Erro ao extrair metadados: {e}")
            # Continuar sem metadados
    
    # Criar devices
    resultado = device_manager.criar_todos_devices(metadados_estacoes)
    
    return DeviceCreationResponse(
        sucesso=resultado["sucesso"],
        falhas=resultado["falhas"],
        devices=[
            DeviceInfo(
                nome=d["nome"],
                device_id=d["device_id"],
                token=d["token"]
            )
            for d in resultado["devices"]
        ]
    )


@router.post("/devices/{estacao}/telemetria/enviar", response_model=TelemetrySendStatus)
async def enviar_telemetria_estacao(
    estacao: str,
    anos: Optional[List[str]] = None,
    background_tasks: BackgroundTasks = None,
    csv_processor=Depends(get_csv_processor),
    device_manager=Depends(get_device_manager)
):
    """
    Envia dados históricos de telemetria para uma estação específica.
    
    Args:
        estacao: Nome da estação (ex: PETROLINA)
        anos: Lista de anos para processar (opcional, padrão: todos)
        
    Returns:
        Status do envio
    """
    import uuid
    
    logger.info(f"Solicitação de envio de telemetria para estação {estacao}")
    
    # Verificar se device existe
    device_info = device_manager.obter_device_por_estacao(estacao)
    
    if not device_info:
        raise HTTPException(
            status_code=404,
            detail=f"Device não encontrado para estação {estacao}. Execute /devices/setup primeiro."
        )
    
    token = device_info.get("token")
    
    if not token:
        raise HTTPException(
            status_code=500,
            detail=f"Token não disponível para device da estação {estacao}"
        )
    
    # Gerar task ID
    task_id = str(uuid.uuid4())
    
    # Iniciar tarefa em background
    if background_tasks:
        telemetry_tasks[task_id] = {
            "status": "processing",
            "estacao": estacao,
            "message": "Processando telemetria..."
        }
        
        background_tasks.add_task(
            enviar_telemetria_task,
            task_id=task_id,
            estacao=estacao,
            token=token,
            anos=anos,
            csv_processor=csv_processor
        )
        
        return TelemetrySendStatus(
            status="started",
            message=f"Envio de telemetria iniciado para {estacao}",
            task_id=task_id
        )
    else:
        # Envio síncrono
        resultado = csv_processor.enviar_telemetria_estacao(
            nome_estacao=estacao,
            device_token=token,
            anos=anos
        )
        
        return TelemetrySendStatus(
            status="completed",
            message=f"Telemetria enviada: {resultado['sucesso']} sucessos, {resultado['falhas']} falhas"
        )


@router.post("/devices/telemetria/enviar-todas")
async def enviar_telemetria_todas(
    anos: Optional[List[str]] = None,
    background_tasks: BackgroundTasks = None,
    csv_processor=Depends(get_csv_processor),
    device_manager=Depends(get_device_manager)
):
    """
    Envia dados históricos de telemetria para todas as estações.
    
    Args:
        anos: Lista de anos para processar (opcional, padrão: todos)
        
    Returns:
        Status do envio
    """
    import uuid
    
    logger.info("Solicitação de envio de telemetria para todas as estações")
    
    # Obter todos os devices
    devices = device_manager.listar_devices()
    
    if not devices:
        raise HTTPException(
            status_code=404,
            detail="Nenhum device encontrado. Execute /devices/setup primeiro."
        )
    
    # Preparar lista de devices
    devices_info = []
    for device in devices:
        # Extrair nome da estação do cache
        for estacao, cached_device in device_manager.devices_cache.items():
            if cached_device == device:
                devices_info.append({
                    "nome": estacao,
                    "token": device.get("token")
                })
                break
    
    # Gerar task ID
    task_id = str(uuid.uuid4())
    
    # Iniciar tarefa em background
    if background_tasks:
        telemetry_tasks[task_id] = {
            "status": "processing",
            "estacoes": len(devices_info),
            "message": "Processando telemetria para todas as estações..."
        }
        
        background_tasks.add_task(
            enviar_telemetria_todas_task,
            task_id=task_id,
            devices_info=devices_info,
            anos=anos,
            csv_processor=csv_processor
        )
        
        return {
            "status": "started",
            "message": f"Envio de telemetria iniciado para {len(devices_info)} estações",
            "task_id": task_id,
            "status_endpoint": f"/api/v1/devices/telemetria/status/{task_id}"
        }
    else:
        # Envio síncrono
        resultado = csv_processor.enviar_telemetria_todas_estacoes(
            devices_info=devices_info,
            anos=anos
        )
        
        return {
            "status": "completed",
            "resultado": resultado
        }


@router.get("/devices/telemetria/status/{task_id}")
async def obter_status_telemetria(task_id: str):
    """
    Obtém o status de uma tarefa de envio de telemetria.
    
    Args:
        task_id: ID da tarefa
        
    Returns:
        Status atual da tarefa
    """
    if task_id not in telemetry_tasks:
        raise HTTPException(
            status_code=404,
            detail=f"Tarefa {task_id} não encontrada"
        )
    
    return telemetry_tasks[task_id]


@router.get("/devices/metadados/{estacao}", response_model=MetadataResponse)
async def obter_metadados_estacao(
    estacao: str,
    metadata_service=Depends(get_metadata_service)
):
    """
    Obtém metadados de gráficos de uma estação.
    
    Args:
        estacao: Nome da estação
        
    Returns:
        Metadados da estação
    """
    try:
        metadados = metadata_service.extrair_metadados_estacao(estacao)
        
        return MetadataResponse(
            estacao=metadados["estacao"],
            total_arquivos=metadados["total_arquivos"],
            modelos_disponiveis=list(metadados["modelos"].keys())
        )
    except Exception as e:
        logger.error(f"Erro ao obter metadados de {estacao}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao obter metadados: {str(e)}"
        )


@router.get("/devices/metadados/resumo")
async def obter_resumo_metadados(
    metadata_service=Depends(get_metadata_service)
):
    """
    Obtém resumo geral dos metadados de todas as estações.
    
    Returns:
        Resumo dos metadados
    """
    try:
        return metadata_service.gerar_resumo_metadados()
    except Exception as e:
        logger.error(f"Erro ao gerar resumo de metadados: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao gerar resumo: {str(e)}"
        )


# Funções auxiliares para background tasks
async def enviar_telemetria_task(
    task_id: str,
    estacao: str,
    token: str,
    anos: Optional[List[str]],
    csv_processor
):
    """Tarefa em background para enviar telemetria de uma estação."""
    try:
        resultado = csv_processor.enviar_telemetria_estacao(
            nome_estacao=estacao,
            device_token=token,
            anos=anos
        )
        
        telemetry_tasks[task_id] = {
            "status": "completed",
            "estacao": estacao,
            "resultado": resultado,
            "message": f"Concluído: {resultado['sucesso']} sucessos, {resultado['falhas']} falhas"
        }
    except Exception as e:
        logger.error(f"Erro ao enviar telemetria para {estacao}: {e}")
        telemetry_tasks[task_id] = {
            "status": "failed",
            "estacao": estacao,
            "error": str(e),
            "message": f"Erro ao processar telemetria: {str(e)}"
        }


async def enviar_telemetria_todas_task(
    task_id: str,
    devices_info: List[Dict],
    anos: Optional[List[str]],
    csv_processor
):
    """Tarefa em background para enviar telemetria de todas as estações."""
    try:
        resultado = csv_processor.enviar_telemetria_todas_estacoes(
            devices_info=devices_info,
            anos=anos
        )
        
        telemetry_tasks[task_id] = {
            "status": "completed",
            "estacoes_processadas": resultado["estacoes_processadas"],
            "resultado": resultado,
            "message": (
                f"Concluído: {resultado['total_sucesso']} sucessos, "
                f"{resultado['total_falhas']} falhas de {resultado['total_registros']} registros"
            )
        }
    except Exception as e:
        logger.error(f"Erro ao enviar telemetria para todas as estações: {e}")
        telemetry_tasks[task_id] = {
            "status": "failed",
            "error": str(e),
            "message": f"Erro ao processar telemetria: {str(e)}"
        }


@router.post("/devices/export-to-s3")
async def exportar_dados_para_s3(
    anos: Optional[List[str]] = None,
    s3_service = Depends(get_s3_service)
):
    """
    Exporta todos os arquivos CSV de todas as estações para o bucket S3.
    Mantém a estrutura original dos arquivos: data/ano/arquivo.CSV
    
    Args:
        anos: Lista opcional de anos específicos (None = todos os anos)
        
    Returns:
        Resumo do processo de upload com detalhes por estação
    """
    try:
        logger.info("Iniciando exportação de dados para S3")
        
        # Verificar se o bucket existe
        if not s3_service.check_bucket_exists():
            raise HTTPException(
                status_code=500,
                detail=f"Bucket S3 '{s3_service.bucket_name}' não está acessível"
            )
        
        # Obter diretório de dados
        data_directory = settings.get_data_directory()
        
        # Fazer upload de todas as estações
        resultado = s3_service.upload_todas_estacoes(
            data_directory=data_directory,
            estacoes=ESTACOES_METEOROLOGICAS,
            anos=anos
        )
        
        return {
            "status": "success",
            "bucket": s3_service.bucket_name,
            "s3_prefix": s3_service.s3_prefix,
            "anos_processados": anos if anos else "todos",
            "resumo": {
                "total_estacoes": resultado['total_estacoes'],
                "total_arquivos": resultado['total_arquivos'],
                "sucesso": resultado['total_sucesso'],
                "falhas": resultado['total_falhas'],
                "taxa_sucesso": f"{(resultado['total_sucesso']/resultado['total_arquivos']*100):.2f}%" if resultado['total_arquivos'] > 0 else "0%"
            },
            "estacoes_processadas": resultado['estacoes_processadas'],
            "detalhes": resultado['detalhes']
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao exportar dados para S3: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao exportar dados para S3: {str(e)}"
        )


@router.post("/devices/export-to-s3/{estacao}")
async def exportar_estacao_para_s3(
    estacao: str,
    anos: Optional[List[str]] = None,
    s3_service = Depends(get_s3_service)
):
    """
    Exporta todos os arquivos CSV de uma estação específica para o bucket S3.
    
    Args:
        estacao: Nome da estação (ex: PETROLINA, ARCO_VERDE)
        anos: Lista opcional de anos específicos (None = todos os anos)
        
    Returns:
        Detalhes do upload da estação
    """
    try:
        # Validar se estação existe
        estacao_upper = estacao.upper()
        estacao_info = next(
            (e for e in ESTACOES_METEOROLOGICAS if e['nome'] == estacao_upper),
            None
        )
        
        if not estacao_info:
            raise HTTPException(
                status_code=404,
                detail=f"Estação '{estacao}' não encontrada"
            )
        
        logger.info(f"Iniciando exportação da estação {estacao_upper} para S3")
        
        # Verificar se o bucket existe
        if not s3_service.check_bucket_exists():
            raise HTTPException(
                status_code=500,
                detail=f"Bucket S3 '{s3_service.bucket_name}' não está acessível"
            )
        
        # Obter diretório de dados
        data_directory = settings.get_data_directory()
        
        # Fazer upload da estação
        resultado = s3_service.upload_todos_csv_estacao(
            data_directory=data_directory,
            estacao_nome=estacao_upper,
            anos=anos
        )
        
        return {
            "status": "success",
            "bucket": s3_service.bucket_name,
            "s3_prefix": s3_service.s3_prefix,
            "estacao": estacao_upper,
            "anos_processados": anos if anos else "todos",
            "resumo": {
                "total_arquivos": resultado['total'],
                "sucesso": resultado['sucesso'],
                "falhas": resultado['falhas'],
                "taxa_sucesso": f"{(resultado['sucesso']/resultado['total']*100):.2f}%" if resultado['total'] > 0 else "0%"
            },
            "arquivos": resultado['arquivos']
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao exportar estação {estacao} para S3: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao exportar estação para S3: {str(e)}"
        )


@router.get("/devices/s3/list")
async def listar_arquivos_s3(
    prefix: Optional[str] = None,
    s3_service = Depends(get_s3_service)
):
    """
    Lista arquivos no bucket S3.
    
    Args:
        prefix: Prefixo opcional para filtrar arquivos
        
    Returns:
        Lista de arquivos no bucket
    """
    try:
        arquivos = s3_service.listar_arquivos_bucket(prefix)
        
        return {
            "status": "success",
            "bucket": s3_service.bucket_name,
            "prefix": prefix if prefix else s3_service.s3_prefix,
            "total_arquivos": len(arquivos),
            "arquivos": arquivos
        }
        
    except Exception as e:
        logger.error(f"Erro ao listar arquivos do S3: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao listar arquivos do S3: {str(e)}"
        )
