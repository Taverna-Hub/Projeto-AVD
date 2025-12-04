"""
Serviço para integração com MLflow.
Fornece funções para tracking de experimentos, logging de métricas e parâmetros.
"""

import mlflow
import os
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class MLflowService:
    """Serviço para gerenciar operações do MLflow."""
    
    def __init__(self):
        """Inicializa o serviço MLflow."""
        self.tracking_uri = os.getenv(
            "MLFLOW_TRACKING_URI", 
            "http://mlflow:5000"
        )
        self.experiment_name = os.getenv(
            "MLFLOW_EXPERIMENT_NAME",
            "data-pipeline"
        )
        self._initialized = False
        
    def initialize(self):
        """Inicializa conexão com MLflow."""
        try:
            mlflow.set_tracking_uri(self.tracking_uri)
            
            # Criar ou obter experimento
            try:
                experiment = mlflow.get_experiment_by_name(self.experiment_name)
                if experiment is None:
                    mlflow.create_experiment(self.experiment_name)
                    logger.info(f"Experimento '{self.experiment_name}' criado no MLflow")
                else:
                    logger.info(f"Usando experimento existente: '{self.experiment_name}'")
            except Exception as e:
                logger.warning(f"Não foi possível criar experimento: {e}")
            
            mlflow.set_experiment(self.experiment_name)
            self._initialized = True
            logger.info(f"MLflow inicializado: {self.tracking_uri}")
            
        except Exception as e:
            logger.error(f"Erro ao inicializar MLflow: {e}")
            self._initialized = False
    
    def start_run(
        self, 
        run_name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> Optional[mlflow.ActiveRun]:
        """
        Inicia uma nova run do MLflow.
        
        Args:
            run_name: Nome da run
            tags: Tags adicionais para a run
            
        Returns:
            ActiveRun object ou None se falhar
        """
        if not self._initialized:
            self.initialize()
        
        try:
            run = mlflow.start_run(run_name=run_name)
            
            # Adicionar tags padrão
            default_tags = {
                "service": "fastapi",
                "timestamp": datetime.now().isoformat()
            }
            
            if tags:
                default_tags.update(tags)
            
            for key, value in default_tags.items():
                mlflow.set_tag(key, value)
            
            return run
            
        except Exception as e:
            logger.error(f"Erro ao iniciar run MLflow: {e}")
            return None
    
    def end_run(self, status: str = "FINISHED"):
        """
        Finaliza a run atual.
        
        Args:
            status: Status da run (FINISHED, FAILED, KILLED)
        """
        try:
            mlflow.end_run(status=status)
        except Exception as e:
            logger.error(f"Erro ao finalizar run MLflow: {e}")
    
    def log_params(self, params: Dict[str, Any]):
        """
        Registra parâmetros no MLflow.
        
        Args:
            params: Dicionário de parâmetros
        """
        if not self._initialized:
            return
        
        try:
            mlflow.log_params(params)
        except Exception as e:
            logger.error(f"Erro ao registrar parâmetros: {e}")
    
    def log_metrics(self, metrics: Dict[str, float], step: Optional[int] = None):
        """
        Registra métricas no MLflow.
        
        Args:
            metrics: Dicionário de métricas
            step: Step opcional para time-series
        """
        if not self._initialized:
            return
        
        try:
            mlflow.log_metrics(metrics, step=step)
        except Exception as e:
            logger.error(f"Erro ao registrar métricas: {e}")
    
    def log_artifact(self, local_path: str, artifact_path: Optional[str] = None):
        """
        Registra um artifact no MLflow.
        
        Args:
            local_path: Caminho local do arquivo
            artifact_path: Caminho relativo no MLflow
        """
        if not self._initialized:
            return
        
        try:
            mlflow.log_artifact(local_path, artifact_path)
        except Exception as e:
            logger.error(f"Erro ao registrar artifact: {e}")
    
    def log_dict(self, dictionary: Dict[str, Any], filename: str):
        """
        Registra um dicionário como artifact JSON.
        
        Args:
            dictionary: Dicionário para salvar
            filename: Nome do arquivo
        """
        if not self._initialized:
            return
        
        try:
            mlflow.log_dict(dictionary, filename)
        except Exception as e:
            logger.error(f"Erro ao registrar dicionário: {e}")
    
    def log_upload_operation(
        self,
        operation_type: str,
        files_count: int,
        success_count: int,
        failed_count: int,
        total_size_mb: float,
        duration_seconds: float,
        year: Optional[int] = None,
        additional_params: Optional[Dict[str, Any]] = None
    ):
        """
        Registra uma operação de upload no MLflow.
        
        Args:
            operation_type: Tipo de operação (upload_all, upload_year, etc)
            files_count: Número total de arquivos
            success_count: Arquivos com sucesso
            failed_count: Arquivos que falharam
            total_size_mb: Tamanho total em MB
            duration_seconds: Duração da operação
            year: Ano específico (opcional)
            additional_params: Parâmetros adicionais
        """
        if not self._initialized:
            self.initialize()
        
        try:
            with mlflow.start_run(run_name=f"{operation_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
                # Tags
                mlflow.set_tag("operation_type", operation_type)
                mlflow.set_tag("timestamp", datetime.now().isoformat())
                
                # Parâmetros
                params = {
                    "files_count": files_count,
                    "operation": operation_type
                }
                
                if year:
                    params["year"] = year
                
                if additional_params:
                    params.update(additional_params)
                
                mlflow.log_params(params)
                
                # Métricas
                metrics = {
                    "success_count": success_count,
                    "failed_count": failed_count,
                    "success_rate": (success_count / files_count * 100) if files_count > 0 else 0,
                    "total_size_mb": total_size_mb,
                    "duration_seconds": duration_seconds,
                    "throughput_mb_per_sec": total_size_mb / duration_seconds if duration_seconds > 0 else 0
                }
                
                mlflow.log_metrics(metrics)
                
                logger.info(f"Operação registrada no MLflow: {operation_type}")
                
        except Exception as e:
            logger.error(f"Erro ao registrar operação de upload: {e}")
    
    def log_imputation_run(
        self,
        station_name: str,
        metrics: Dict[str, float],
        params: Dict[str, Any],
        artifacts: Optional[Dict[str, str]] = None
    ):
        """
        Registra uma run de imputação no MLflow.
        
        Args:
            station_name: Nome da estação
            metrics: Métricas da imputação (RMSE, MAE, R2, etc)
            params: Parâmetros do modelo
            artifacts: Caminhos de artifacts para registrar (plot, metrics_file, etc)
        """
        if not self._initialized:
            self.initialize()
        
        try:
            run_name = f"imputation_{station_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            with mlflow.start_run(run_name=run_name):
                # Tags
                mlflow.set_tag("pipeline", "data_imputation")
                mlflow.set_tag("station", station_name)
                mlflow.set_tag("station_name", station_name)
                mlflow.set_tag("timestamp", datetime.now().isoformat())
                mlflow.set_tag("type", "imputation")
                
                # Parâmetros
                mlflow.log_params(params)
                
                # Métricas
                mlflow.log_metrics(metrics)
                
                # Artifacts
                if artifacts:
                    for artifact_name, artifact_path in artifacts.items():
                        if artifact_path and Path(artifact_path).exists():
                            mlflow.log_artifact(artifact_path)
                
                logger.info(f"Run de imputação registrada: {station_name}")
                
        except Exception as e:
            logger.error(f"Erro ao registrar run de imputação: {e}")


# Instância global do serviço
mlflow_service = MLflowService()
