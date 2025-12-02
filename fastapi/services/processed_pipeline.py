"""
Serviço de pipeline de dados tratados.
Coordena o fluxo: Dados Tratados → S3 → MLflow → ThingsBoard/Trendz
"""

import pandas as pd
import numpy as np
import pickle
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
import tempfile
import os

from .s3_service import S3Service
from .mlflow_service import MLflowService
from .thingsboard_service import ThingsBoardService
from .trendz_service import TrendzService

logger = logging.getLogger(__name__)


class ProcessedDataPipeline:
    """Pipeline para processar e distribuir dados meteorológicos tratados."""
    
    def __init__(
        self,
        s3_service: S3Service,
        mlflow_service: MLflowService,
        tb_service: Optional[ThingsBoardService] = None,
        trendz_service: Optional[TrendzService] = None
    ):
        """
        Inicializa o pipeline.
        
        Args:
            s3_service: Serviço S3
            mlflow_service: Serviço MLflow
            tb_service: Serviço ThingsBoard (opcional)
            trendz_service: Serviço Trendz (opcional)
        """
        self.s3_service = s3_service
        self.mlflow_service = mlflow_service
        self.tb_service = tb_service
        self.trendz_service = trendz_service
        
    def process_and_export_notebook_results(
        self,
        results_pkl_path: str,
        station_name: str = "A307_PETROLINA",
        export_to_tb: bool = True,
        export_to_trendz: bool = True,
        device_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Processa resultados do notebook e exporta para S3, MLflow, ThingsBoard e Trendz.
        
        Args:
            results_pkl_path: Caminho para o arquivo results.pkl do notebook
            station_name: Nome da estação meteorológica
            export_to_tb: Se deve exportar para ThingsBoard
            export_to_trendz: Se deve exportar para Trendz
            device_token: Token do dispositivo no ThingsBoard
            
        Returns:
            Dict com estatísticas do processamento
        """
        start_time = datetime.now()
        
        try:
            # 1. Carregar resultados do notebook
            logger.info(f"Carregando resultados de {results_pkl_path}")
            with open(results_pkl_path, 'rb') as f:
                results = pickle.load(f)
            
            # Extrair dados
            model = results['model']
            scaler = results['scaler']
            X_test = results['X_test']
            y_test = results['y_test']
            y_pred_test = results['y_pred_test']
            rmse_train = results['rmse_train']
            rmse_test = results['rmse_test']
            
            # 2. Criar DataFrames processados
            df_processed = self._create_processed_dataframe(
                X_test, y_test, y_pred_test
            )
            
            # 3. Salvar em CSV temporário
            temp_dir = tempfile.mkdtemp()
            csv_path = Path(temp_dir) / f"{station_name}_processed_data.csv"
            df_processed.to_csv(csv_path, index=True)
            
            # 4. Upload para S3 (pasta de dados tratados)
            logger.info("Enviando dados processados para S3...")
            s3_result = self._upload_to_s3_processed(
                csv_path, station_name
            )
            
            # 5. Registrar no MLflow
            logger.info("Registrando experimento no MLflow...")
            mlflow_result = self._log_to_mlflow(
                results, df_processed, station_name, csv_path
            )
            
            # 6. Enviar para ThingsBoard (opcional)
            tb_result = None
            if export_to_tb and self.tb_service and device_token:
                logger.info("Enviando dados para ThingsBoard...")
                tb_result = self._send_to_thingsboard(
                    df_processed, device_token
                )
            
            # 7. Sincronizar com Trendz (opcional)
            trendz_result = None
            if export_to_trendz and self.trendz_service:
                logger.info("Sincronizando com Trendz Analytics...")
                trendz_result = self._sync_to_trendz(station_name)
            
            # Limpar arquivos temporários
            os.remove(csv_path)
            os.rmdir(temp_dir)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            return {
                "success": True,
                "station_name": station_name,
                "records_processed": len(df_processed),
                "duration_seconds": duration,
                "s3_upload": s3_result,
                "mlflow_run": mlflow_result,
                "thingsboard_export": tb_result,
                "trendz_sync": trendz_result,
                "metrics": {
                    "rmse_train": float(rmse_train),
                    "rmse_test": float(rmse_test),
                    "predictions_count": len(y_pred_test)
                }
            }
            
        except Exception as e:
            logger.error(f"Erro no pipeline: {e}")
            return {
                "success": False,
                "error": str(e),
                "duration_seconds": (datetime.now() - start_time).total_seconds()
            }
    
    def _create_processed_dataframe(
        self,
        X_test: pd.DataFrame,
        y_test: pd.Series,
        y_pred_test: np.ndarray
    ) -> pd.DataFrame:
        """Cria DataFrame com dados processados e predições."""
        df = X_test.copy()
        df['temperatura_real'] = y_test.values
        df['temperatura_prevista'] = y_pred_test
        df['erro_predicao'] = y_test.values - y_pred_test
        df['erro_absoluto'] = np.abs(df['erro_predicao'])
        
        # Adicionar timestamp se não existir
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.date_range(
                start='2023-01-01',
                periods=len(df),
                freq='H'
            )
        
        return df
    
    def _upload_to_s3_processed(
        self,
        csv_path: Path,
        station_name: str
    ) -> Dict[str, Any]:
        """Upload de dados processados para pasta específica no S3."""
        try:
            # Upload para pasta 'processed-data' no bucket
            original_prefix = self.s3_service.s3_prefix
            self.s3_service.s3_prefix = f"{original_prefix}/processed-data"
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            s3_key = f"{station_name}/{timestamp}_processed.csv"
            
            result = self.s3_service.upload_file(
                str(csv_path),
                s3_key=s3_key
            )
            
            # Restaurar prefix original
            self.s3_service.s3_prefix = original_prefix
            
            return result
            
        except Exception as e:
            logger.error(f"Erro ao fazer upload para S3: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def _log_to_mlflow(
        self,
        results: Dict[str, Any],
        df_processed: pd.DataFrame,
        station_name: str,
        csv_path: Path
    ) -> Dict[str, Any]:
        """Registra experimento no MLflow."""
        try:
            if not self.mlflow_service._initialized:
                self.mlflow_service.initialize()
            
            run_name = f"processed_data_{station_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            with self.mlflow_service.start_run(
                run_name=run_name,
                tags={
                    "pipeline": "processed_data",
                    "station": station_name,
                    "type": "weather_prediction"
                }
            ):
                # Log parâmetros
                self.mlflow_service.log_params({
                    "station_name": station_name,
                    "records_count": len(df_processed),
                    "features_count": len(results['X_test'].columns),
                    "model_type": type(results['model']).__name__
                })
                
                # Log métricas
                self.mlflow_service.log_metrics({
                    "rmse_train": float(results['rmse_train']),
                    "rmse_test": float(results['rmse_test']),
                    "predictions_count": len(results['y_pred_test']),
                    "mean_abs_error": float(df_processed['erro_absoluto'].mean()),
                    "max_abs_error": float(df_processed['erro_absoluto'].max()),
                    "min_abs_error": float(df_processed['erro_absoluto'].min())
                })
                
                # Log artifact (CSV processado)
                self.mlflow_service.log_artifact(str(csv_path), "processed_data")
                
                # Log estatísticas descritivas
                stats_dict = df_processed.describe().to_dict()
                self.mlflow_service.log_dict(stats_dict, "statistics.json")
                
                run_info = {
                    "success": True,
                    "run_name": run_name
                }
                
                self.mlflow_service.end_run(status="FINISHED")
                
                return run_info
            
        except Exception as e:
            logger.error(f"Erro ao registrar no MLflow: {e}")
            if self.mlflow_service:
                self.mlflow_service.end_run(status="FAILED")
            return {
                "success": False,
                "error": str(e)
            }
    
    def _send_to_thingsboard(
        self,
        df: pd.DataFrame,
        device_token: str
    ) -> Dict[str, Any]:
        """Envia dados processados para ThingsBoard."""
        try:
            # Selecionar apenas colunas relevantes para telemetria
            telemetry_columns = [
                'temperatura', 'umidade', 'vento_velocidade',
                'radiacao', 'precipitacao', 'temperatura_real',
                'temperatura_prevista', 'erro_predicao'
            ]
            
            df_telemetry = df[[col for col in telemetry_columns if col in df.columns]]
            
            result = self.tb_service.send_dataframe(
                device_token=device_token,
                df=df_telemetry,
                timestamp_column=None,  # Usar índice
                batch_size=100
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Erro ao enviar para ThingsBoard: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def _sync_to_trendz(self, station_name: str) -> Dict[str, Any]:
        """Sincroniza dados com Trendz Analytics."""
        try:
            # Sincronizar fontes de dados
            sync_success = self.trendz_service.sync_data_sources()
            
            return {
                "success": sync_success,
                "station_name": station_name
            }
            
        except Exception as e:
            logger.error(f"Erro ao sincronizar com Trendz: {e}")
            return {
                "success": False,
                "error": str(e)
            }


def create_pipeline(
    bucket_name: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    aws_region: str = "us-east-1",
    s3_prefix: str = "inmet-data",
    mlflow_uri: str = "http://mlflow:5000",
    tb_url: str = "http://thingsboard:9090",
    trendz_url: str = "http://trendz:8888"
) -> ProcessedDataPipeline:
    """
    Factory function para criar pipeline configurado.
    
    Args:
        bucket_name: Nome do bucket S3
        aws_access_key_id: AWS access key
        aws_secret_access_key: AWS secret key
        aws_region: Região AWS
        s3_prefix: Prefixo no S3
        mlflow_uri: URI do MLflow
        tb_url: URL do ThingsBoard
        trendz_url: URL do Trendz
        
    Returns:
        ProcessedDataPipeline configurado
    """
    # Inicializar serviços
    s3_service = S3Service(
        bucket_name=bucket_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_region=aws_region,
        s3_prefix=s3_prefix
    )
    
    mlflow_service = MLflowService()
    mlflow_service.tracking_uri = mlflow_uri
    mlflow_service.initialize()
    
    tb_service = ThingsBoardService(tb_url=tb_url)
    trendz_service = TrendzService(trendz_url=trendz_url, tb_url=tb_url)
    
    return ProcessedDataPipeline(
        s3_service=s3_service,
        mlflow_service=mlflow_service,
        tb_service=tb_service,
        trendz_service=trendz_service
    )
