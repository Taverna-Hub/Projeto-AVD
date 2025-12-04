"""
Serviço de monitoramento do MLflow.
Monitora novos runs/experimentos e sincroniza dados processados com ThingsBoard.

Este módulo pode ser usado de 3 formas:
1. Como serviço importado: from services.mlflow_monitor import mlflow_monitor
2. Como API REST: endpoints em /api/v1/mlflow-sync/*
3. Como script CLI: python -m services.mlflow_monitor [OPTIONS]

Uso CLI:
    python -m services.mlflow_monitor [OPTIONS]

Options:
    --experiments, -e   : Lista de experimentos para monitorar (separados por vírgula)
    --interval, -i      : Intervalo de verificação em segundos (default: 60)
    --daemon, -d        : Rodar em modo daemon (loop contínuo)
    --tb-url            : URL do ThingsBoard (default: http://thingsboard:9090)
    --tb-user           : Usuário do ThingsBoard
    --tb-password       : Senha do ThingsBoard
    --mlflow-uri        : URI do MLflow (default: http://mlflow:5000)
    --log-level         : Nível de log (DEBUG, INFO, WARNING, ERROR)
"""

import mlflow
import logging
import pandas as pd
import pickle
import time
import sys
import os
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

# Para compatibilidade de importação
if __name__ == "__main__":
    # Quando rodado como script
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from services.mlflow_service import mlflow_service
    from services.thingsboard_service import thingsboard_service
else:
    # Quando importado como módulo
    from .mlflow_service import mlflow_service
    from .thingsboard_service import thingsboard_service

logger = logging.getLogger(__name__)


class MLflowMonitor:
    """Monitor de eventos do MLflow para sincronização com ThingsBoard."""
    
    def __init__(
        self,
        mlflow_tracking_uri: str = "http://mlflow:5000",
        check_interval: int = 60,
        tb_username: Optional[str] = None,
        tb_password: Optional[str] = None
    ):
        """
        Inicializa o monitor do MLflow.
        
        Args:
            mlflow_tracking_uri: URI do MLflow
            check_interval: Intervalo de verificação em segundos
            tb_username: Usuário do ThingsBoard
            tb_password: Senha do ThingsBoard
        """
        self.mlflow_tracking_uri = mlflow_tracking_uri
        self.check_interval = check_interval
        self.tb_username = tb_username or os.getenv("THINGSBOARD_USERNAME", "tenant@thingsboard.org")
        self.tb_password = tb_password or os.getenv("THINGSBOARD_PASSWORD", "tenant")
        
        # Último timestamp verificado por experimento
        self.last_check_timestamps: Dict[str, int] = {}
        
        # Cache de devices criados
        self.device_cache: Dict[str, Dict[str, Any]] = {}
        
        self._running = False
        self._authenticated_tb = False
    
    def initialize(self) -> bool:
        """
        Inicializa conexão com MLflow e ThingsBoard.
        
        Returns:
            True se inicializado com sucesso
        """
        try:
            # Inicializar MLflow
            mlflow.set_tracking_uri(self.mlflow_tracking_uri)
            logger.info(f"MLflow conectado: {self.mlflow_tracking_uri}")
            
            # Autenticar no ThingsBoard
            if thingsboard_service.authenticate(self.tb_username, self.tb_password):
                self._authenticated_tb = True
                logger.info("ThingsBoard autenticado com sucesso")
                return True
            else:
                logger.error("Falha ao autenticar no ThingsBoard")
                return False
                
        except Exception as e:
            logger.error(f"Erro ao inicializar monitor: {e}")
            return False
    
    def get_new_runs(self, experiment_name: str) -> List[mlflow.entities.Run]:
        """
        Busca novos runs desde a última verificação.
        
        Args:
            experiment_name: Nome do experimento
            
        Returns:
            Lista de runs novos
        """
        try:
            experiment = mlflow.get_experiment_by_name(experiment_name)
            if not experiment:
                return []
            
            experiment_id = experiment.experiment_id
            
            # Timestamp da última verificação
            last_check = self.last_check_timestamps.get(experiment_id, 0)
            
            # Buscar runs do experimento
            runs = mlflow.search_runs(
                experiment_ids=[experiment_id],
                order_by=["start_time DESC"],
                max_results=100
            )
            
            if runs.empty:
                return []
            
            # Filtrar runs novos
            new_runs = []
            for idx, run_row in runs.iterrows():
                run_id = run_row['run_id']
                start_time = run_row['start_time']
                run_name = run_row.get('tags.mlflow.runName', 'unknown')
                
                # Converter start_time para timestamp
                if isinstance(start_time, pd.Timestamp):
                    start_timestamp = int(start_time.timestamp() * 1000)
                else:
                    start_timestamp = int(start_time)
                
                if start_timestamp > last_check:
                    run = mlflow.get_run(run_id)
                    new_runs.append(run)
                    logger.info(f"Nova run detectada: {run_name[:50]} ({run_id[:8]}...)")
            
            # Atualizar último timestamp verificado
            if new_runs:
                latest_timestamp = max(
                    int(run.info.start_time) for run in new_runs
                )
                self.last_check_timestamps[experiment_id] = latest_timestamp
                logger.info(f"Detectadas {len(new_runs)} nova(s) run(s)")
            
            return new_runs
            
        except Exception as e:
            logger.error(f"Erro ao buscar novos runs: {e}")
            return []
    
    def extract_station_from_run(self, run: mlflow.entities.Run) -> Optional[str]:
        """
        Extrai o nome da estação de um run do MLflow.
        
        Args:
            run: Run do MLflow
            
        Returns:
            Nome da estação ou None
        """
        # Tentar extrair de tags
        tags = run.data.tags
        logger.debug(f"Tags disponíveis: {list(tags.keys())}")
        
        if "station" in tags:
            logger.debug(f"Estação encontrada em tag 'station': {tags['station']}")
            return tags["station"]
        
        if "station_name" in tags:
            logger.debug(f"Estação encontrada em tag 'station_name': {tags['station_name']}")
            return tags["station_name"]
        
        # Tentar extrair de parâmetros
        params = run.data.params
        if "station_name" in params:
            return params["station_name"]
        
        # Tentar extrair do nome do run
        run_name = run.info.run_name
        if run_name:
            # Formato: "processed_data_ESTACAO_timestamp"
            if "processed_data_" in run_name:
                parts = run_name.split("_")
                if len(parts) >= 3:
                    # Extrai entre "data" e timestamp
                    station_parts = parts[2:-2] if parts[-1].isdigit() else parts[2:]
                    return "_".join(station_parts)
            
            # Formato: "imputacao_ESTACAO_timestamp" ou similar
            for keyword in ["imputacao", "imputation", "estacao", "station"]:
                if keyword in run_name.lower():
                    parts = run_name.split("_")
                    idx = next((i for i, p in enumerate(parts) if keyword in p.lower()), -1)
                    if idx >= 0 and idx + 1 < len(parts):
                        return parts[idx + 1]
        
        return None
    
    def download_pkl_artifacts(self, run: mlflow.entities.Run) -> List[Path]:
        """
        Baixa arquivos .pkl dos artifacts de um run.
        
        Args:
            run: Run do MLflow
            
        Returns:
            Lista de caminhos dos arquivos baixados
        """
        try:
            run_id = run.info.run_id
            artifact_uri = run.info.artifact_uri
            
            # Listar artifacts
            client = mlflow.tracking.MlflowClient()
            artifacts = client.list_artifacts(run_id)
            
            pkl_files = []
            temp_dir = Path("/tmp/mlflow_monitor")
            temp_dir.mkdir(exist_ok=True)
            
            for artifact in artifacts:
                if artifact.path.endswith('.pkl'):
                    # Baixar artifact
                    local_path = temp_dir / f"{run_id}_{artifact.path}"
                    downloaded_path = client.download_artifacts(run_id, artifact.path, str(temp_dir))
                    pkl_files.append(Path(downloaded_path))
                    logger.info(f"Artifact baixado: {artifact.path}")
            
            return pkl_files
            
        except Exception as e:
            logger.error(f"Erro ao baixar artifacts: {e}")
            return []
    
    def extract_station_name_from_pkl(self, pkl_path: Path) -> Optional[str]:
        """
        Extrai o nome da estação de um arquivo .pkl.
        
        Args:
            pkl_path: Caminho do arquivo .pkl
            
        Returns:
            Nome da estação ou None
        """
        try:
            # Tentar extrair do nome do arquivo
            filename = pkl_path.stem
            
            # Padrões comuns: dados_imputados_ESTACAO, dados_tratados_ESTACAO
            for prefix in ["dados_imputados_", "dados_tratados_", "dados_processados_"]:
                if prefix in filename:
                    station = filename.replace(prefix, "").strip()
                    if station:
                        return station
            
            # Tentar carregar e extrair metadata
            try:
                with open(pkl_path, 'rb') as f:
                    data = pickle.load(f)
                
                # Se for dict com metadata
                if isinstance(data, dict):
                    if "estacao" in data:
                        return data["estacao"]
                    if "station_name" in data:
                        return data["station_name"]
                    if "metadata" in data and isinstance(data["metadata"], dict):
                        meta = data["metadata"]
                        if "estacao" in meta:
                            return meta["estacao"]
                        if "station_name" in meta:
                            return meta["station_name"]
            except:
                pass
            
            return None
            
        except Exception as e:
            logger.error(f"Erro ao extrair nome da estação: {e}")
            return None
    
    def load_processed_data(self, pkl_path: Path) -> Optional[pd.DataFrame]:
        """
        Carrega dados processados de um arquivo .pkl.
        
        Args:
            pkl_path: Caminho do arquivo .pkl
            
        Returns:
            DataFrame com os dados ou None
        """
        try:
            with open(pkl_path, 'rb') as f:
                data = pickle.load(f)
            
            # Se já é DataFrame
            if isinstance(data, pd.DataFrame):
                return data
            
            # Se é dict com DataFrame
            if isinstance(data, dict):
                if "data" in data and isinstance(data["data"], pd.DataFrame):
                    return data["data"]
                if "df" in data and isinstance(data["df"], pd.DataFrame):
                    return data["df"]
                if "dataframe" in data and isinstance(data["dataframe"], pd.DataFrame):
                    return data["dataframe"]
            
            logger.warning(f"Arquivo {pkl_path} não contém DataFrame")
            return None
            
        except Exception as e:
            logger.error(f"Erro ao carregar dados: {e}")
            return None
    
    def get_or_create_device(self, station_name: str) -> Optional[Dict[str, Any]]:
        """
        Obtém ou cria device no ThingsBoard para a estação.
        
        Args:
            station_name: Nome da estação
            
        Returns:
            Informações do device (id, token) ou None
        """
        try:
            # Formatar nome do device
            device_name = f"{station_name.replace('_', ' ')} - Processado"
            
            # Verificar cache
            if device_name in self.device_cache:
                logger.info(f"Device encontrado no cache: {device_name}")
                return self.device_cache[device_name]
            
            # Verificar se device já existe no ThingsBoard
            if not self._authenticated_tb:
                logger.error("ThingsBoard não autenticado")
                return None
            
            # Buscar device existente
            try:
                url = f"{thingsboard_service.tb_url}/api/tenant/devices?deviceName={device_name}"
                headers = {"Authorization": f"Bearer {thingsboard_service.jwt_token}"}
                
                import requests
                response = requests.get(url, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    device_info = response.json()
                    if device_info:
                        device_id = device_info.get("id", {}).get("id")
                        if device_id:
                            # Obter token
                            token = thingsboard_service.get_device_credentials(device_id)
                            if token:
                                device_data = {
                                    "id": device_id,
                                    "name": device_name,
                                    "token": token,
                                    "info": device_info
                                }
                                self.device_cache[device_name] = device_data
                                logger.info(f"Device existente encontrado: {device_name}")
                                return device_data
            except:
                pass
            
            # Criar novo device
            logger.info(f"Criando novo device: {device_name}")
            device_info = thingsboard_service.create_device(
                device_name=device_name,
                device_type="weather_station_processed",
                device_label=f"Dados Processados - {station_name}"
            )
            
            if not device_info:
                logger.error(f"Falha ao criar device: {device_name}")
                return None
            
            device_id = device_info.get("id", {}).get("id")
            if not device_id:
                logger.error("Device criado mas ID não retornado")
                return None
            
            # Obter token
            token = thingsboard_service.get_device_credentials(device_id)
            if not token:
                logger.error("Falha ao obter token do device")
                return None
            
            device_data = {
                "id": device_id,
                "name": device_name,
                "token": token,
                "info": device_info
            }
            
            self.device_cache[device_name] = device_data
            logger.info(f"Device criado com sucesso: {device_name}")
            
            return device_data
            
        except Exception as e:
            logger.error(f"Erro ao obter/criar device: {e}")
            return None
    
    def send_data_to_thingsboard(
        self,
        device_token: str,
        df: pd.DataFrame,
        station_name: str
    ) -> Dict[str, Any]:
        """
        Envia dados processados para o ThingsBoard.
        
        Args:
            device_token: Token do device
            df: DataFrame com dados processados
            station_name: Nome da estação
            
        Returns:
            Estatísticas do envio
        """
        try:
            # Verificar se há coluna de timestamp
            timestamp_col = None
            for col in ['timestamp', 'data', 'hora', 'datetime']:
                if col in df.columns:
                    timestamp_col = col
                    break
            
            # Limitar número de registros (evitar sobrecarga)
            max_records = 1000
            if len(df) > max_records:
                logger.warning(f"Dataset muito grande ({len(df)}), enviando apenas {max_records} registros")
                df = df.tail(max_records)
            
            result = thingsboard_service.send_dataframe(
                device_token=device_token,
                df=df,
                timestamp_column=timestamp_col,
                batch_size=100
            )
            
            logger.info(
                f"Dados enviados para ThingsBoard - {station_name}: "
                f"{result['success']}/{result['total']} registros"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Erro ao enviar dados para ThingsBoard: {e}")
            return {
                "success": 0,
                "failed": 0,
                "total": 0,
                "error": str(e)
            }
    
    def process_run(self, run: mlflow.entities.Run) -> Dict[str, Any]:
        """
        Processa um run do MLflow: extrai dados e envia para ThingsBoard.
        
        Args:
            run: Run do MLflow
            
        Returns:
            Resultado do processamento
        """
        result = {
            "run_id": run.info.run_id,
            "run_name": run.info.run_name,
            "success": False,
            "station_name": None,
            "device_created": False,
            "data_sent": False,
            "records_sent": 0,
            "error": None
        }
        
        try:
            # 1. Extrair nome da estação
            station_name = self.extract_station_from_run(run)
            if not station_name:
                result["error"] = "Não foi possível extrair nome da estação"
                logger.warning(f"Run {run.info.run_id}: {result['error']}")
                return result
            
            result["station_name"] = station_name
            logger.info(f"Processando run para estação: {station_name}")
            
            # 2. Obter ou criar device no ThingsBoard (sempre cria, mesmo sem dados)
            device = self.get_or_create_device(station_name)
            if not device:
                result["error"] = "Falha ao criar/obter device no ThingsBoard"
                logger.error(f"Run {run.info.run_id}: {result['error']}")
                return result
            
            result["device_created"] = True
            result["device_id"] = device["id"]
            result["success"] = True
            
            logger.info(f"Device criado com sucesso para estação: {station_name}")
            
            # 3. Tentar baixar e enviar dados (opcional para teste)
            pkl_files = self.download_pkl_artifacts(run)
            if pkl_files:
                df_processed = None
                for pkl_file in pkl_files:
                    df = self.load_processed_data(pkl_file)
                    if df is not None and not df.empty:
                        df_processed = df
                        logger.info(f"Dados carregados: {len(df)} registros de {pkl_file.name}")
                        break
                
                if df_processed is not None:
                    # Enviar dados para ThingsBoard
                    send_result = self.send_data_to_thingsboard(
                        device_token=device["token"],
                        df=df_processed,
                        station_name=station_name
                    )
                    result["data_sent"] = send_result["success"] > 0
                    result["records_sent"] = send_result["success"]
                    logger.info(f"Dados enviados: {result['records_sent']} registros")
            
            logger.info(f"Run processado com sucesso: {station_name}")
            
        except Exception as e:
            result["error"] = str(e)
            logger.error(f"Erro ao processar run {run.info.run_id}: {e}")
        
        return result
    
    def check_for_updates(self, experiment_names: List[str]) -> List[Dict[str, Any]]:
        """
        Verifica atualizações em experimentos do MLflow.
        
        Args:
            experiment_names: Lista de nomes de experimentos para monitorar
            
        Returns:
            Lista de resultados de processamento
        """
        results = []
        
        for experiment_name in experiment_names:
            try:
                logger.info(f"Verificando experimento: {experiment_name}")
                
                new_runs = self.get_new_runs(experiment_name)
                
                if not new_runs:
                    logger.debug(f"Nenhum run novo em {experiment_name}")
                    continue
                
                logger.info(f"Encontrados {len(new_runs)} novos runs em {experiment_name}")
                
                for run in new_runs:
                    result = self.process_run(run)
                    results.append(result)
                    
                    # Delay entre processamentos
                    time.sleep(2)
                    
            except Exception as e:
                logger.error(f"Erro ao verificar experimento {experiment_name}: {e}")
        
        return results
    
    def start_monitoring(
        self,
        experiment_names: List[str],
        daemon: bool = False
    ):
        """
        Inicia monitoramento contínuo do MLflow.
        
        Args:
            experiment_names: Lista de experimentos para monitorar
            daemon: Se True, roda em modo daemon (loop infinito)
        """
        if not self.initialize():
            logger.error("Falha ao inicializar monitor")
            return
        
        self._running = True
        logger.info(f"Monitor iniciado. Verificando a cada {self.check_interval}s")
        
        try:
            while self._running:
                results = self.check_for_updates(experiment_names)
                
                if results:
                    success_count = sum(1 for r in results if r["success"])
                    logger.info(
                        f"Ciclo de verificação completo: "
                        f"{success_count}/{len(results)} runs processados com sucesso"
                    )
                
                if not daemon:
                    break
                
                time.sleep(self.check_interval)
                
        except KeyboardInterrupt:
            logger.info("Monitor interrompido pelo usuário")
        finally:
            self._running = False
            logger.info("Monitor finalizado")
    
    def stop_monitoring(self):
        """Para o monitoramento."""
        self._running = False


# Instância global do monitor
mlflow_monitor = MLflowMonitor()


# ============================================================================
# CLI Interface
# ============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Monitor MLflow e sincronizar dados com ThingsBoard',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--experiments', '-e',
        type=str,
        default='data-pipeline,Imputacao por Estacao',
        help='Lista de experimentos para monitorar (separados por vírgula)'
    )
    
    parser.add_argument(
        '--interval', '-i',
        type=int,
        default=60,
        help='Intervalo de verificação em segundos'
    )
    
    parser.add_argument(
        '--daemon', '-d',
        action='store_true',
        help='Rodar em modo daemon (loop contínuo)'
    )
    
    parser.add_argument(
        '--tb-url',
        type=str,
        default=os.getenv('THINGSBOARD_URL', 'http://thingsboard:9090'),
        help='URL do ThingsBoard'
    )
    
    parser.add_argument(
        '--tb-user',
        type=str,
        default=os.getenv('THINGSBOARD_USERNAME', 'tenant@thingsboard.org'),
        help='Usuário do ThingsBoard'
    )
    
    parser.add_argument(
        '--tb-password',
        type=str,
        default=os.getenv('THINGSBOARD_PASSWORD', 'tenant'),
        help='Senha do ThingsBoard'
    )
    
    parser.add_argument(
        '--mlflow-uri',
        type=str,
        default=os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow:5000'),
        help='URI do MLflow'
    )
    
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Nível de log'
    )
    
    return parser.parse_args()


def main():
    """Main function para execução CLI."""
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('/tmp/mlflow_monitor.log')
        ]
    )
    
    args = parse_args()
    
    # Configurar nível de log
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Parse lista de experimentos
    experiment_names = [e.strip() for e in args.experiments.split(',')]
    
    logger.info("=" * 70)
    logger.info("MLflow Monitor - Sincronização com ThingsBoard")
    logger.info("=" * 70)
    logger.info(f"MLflow URI: {args.mlflow_uri}")
    logger.info(f"ThingsBoard URL: {args.tb_url}")
    logger.info(f"Experimentos: {experiment_names}")
    logger.info(f"Intervalo: {args.interval}s")
    logger.info(f"Modo daemon: {args.daemon}")
    logger.info("=" * 70)
    
    # Configurar monitor
    mlflow_monitor.mlflow_tracking_uri = args.mlflow_uri
    mlflow_monitor.check_interval = args.interval
    mlflow_monitor.tb_username = args.tb_user
    mlflow_monitor.tb_password = args.tb_password
    
    # Atualizar URL do ThingsBoard
    thingsboard_service.tb_url = args.tb_url.rstrip('/')
    
    try:
        # Iniciar monitoramento
        mlflow_monitor.start_monitoring(
            experiment_names=experiment_names,
            daemon=args.daemon
        )
        
        if not args.daemon:
            logger.info("Verificação única completa")
        
    except KeyboardInterrupt:
        logger.info("\nMonitor interrompido pelo usuário")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Erro fatal: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()


# ============================================================================
# FastAPI Router
# ============================================================================

try:
    from fastapi import APIRouter, HTTPException, BackgroundTasks
    from pydantic import BaseModel, Field
    
    router = APIRouter(prefix="/mlflow-sync", tags=["MLflow Sync"])
    
    
    class StartMonitorRequest(BaseModel):
        """Request para iniciar monitoramento."""
        experiment_names: List[str] = Field(
            default=["data-pipeline", "Imputacao por Estacao"],
            description="Lista de experimentos para monitorar"
        )
        check_interval: int = Field(
            default=60,
            ge=10,
            le=3600,
            description="Intervalo de verificação em segundos"
        )
        daemon: bool = Field(
            default=False,
            description="Rodar em modo daemon (background)"
        )
    
    
    class MonitorStatusResponse(BaseModel):
        """Response do status do monitor."""
        running: bool
        experiment_names: Optional[List[str]] = None
        last_check_timestamps: Dict[str, int]
        device_cache_size: int
    
    
    class SyncResultResponse(BaseModel):
        """Response de sincronização manual."""
        success: bool
        message: str
        results: List[Dict[str, Any]]
        total_runs_processed: int
        successful_syncs: int
        failed_syncs: int
    
    
    @router.post("/start", response_model=Dict[str, Any])
    async def start_monitoring(
        request: StartMonitorRequest,
        background_tasks: BackgroundTasks
    ):
        """
        Inicia o monitoramento do MLflow.
        
        O monitor verificará periodicamente por novos runs nos experimentos
        especificados e enviará os dados processados para o ThingsBoard.
        """
        try:
            if mlflow_monitor._running:
                return {
                    "success": False,
                    "message": "Monitor já está em execução",
                    "running": True
                }
            
            # Atualizar configurações
            mlflow_monitor.check_interval = request.check_interval
            
            if request.daemon:
                # Rodar em background
                background_tasks.add_task(
                    mlflow_monitor.start_monitoring,
                    experiment_names=request.experiment_names,
                    daemon=True
                )
                
                return {
                    "success": True,
                    "message": "Monitor iniciado em background",
                    "running": True,
                    "experiment_names": request.experiment_names,
                    "check_interval": request.check_interval,
                    "mode": "daemon"
                }
            else:
                # Executar uma verificação única
                if not mlflow_monitor.initialize():
                    raise HTTPException(
                        status_code=500,
                        detail="Falha ao inicializar monitor"
                    )
                
                results = mlflow_monitor.check_for_updates(request.experiment_names)
                
                success_count = sum(1 for r in results if r.get("success", False))
                
                return {
                    "success": True,
                    "message": f"Verificação única completa: {success_count}/{len(results)} runs processados",
                    "running": False,
                    "results": results,
                    "total_runs": len(results),
                    "successful": success_count,
                    "failed": len(results) - success_count
                }
                
        except Exception as e:
            logger.error(f"Erro ao iniciar monitor: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    
    @router.post("/stop")
    async def stop_monitoring():
        """
        Para o monitoramento do MLflow.
        """
        try:
            if not mlflow_monitor._running:
                return {
                    "success": False,
                    "message": "Monitor não está em execução",
                    "running": False
                }
            
            mlflow_monitor.stop_monitoring()
            
            return {
                "success": True,
                "message": "Monitor parado com sucesso",
                "running": False
            }
            
        except Exception as e:
            logger.error(f"Erro ao parar monitor: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    
    @router.get("/status", response_model=MonitorStatusResponse)
    async def get_status():
        """
        Obtém o status atual do monitor.
        """
        try:
            return MonitorStatusResponse(
                running=mlflow_monitor._running,
                last_check_timestamps=mlflow_monitor.last_check_timestamps,
                device_cache_size=len(mlflow_monitor.device_cache)
            )
            
        except Exception as e:
            logger.error(f"Erro ao obter status: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    
    @router.post("/sync-now", response_model=SyncResultResponse)
    async def sync_now(experiment_names: List[str] = ["data-pipeline", "Imputacao por Estacao"]):
        """
        Executa sincronização imediata (não inicia daemon).
        
        Verifica por novos runs nos experimentos especificados e
        envia dados para o ThingsBoard.
        """
        try:
            # Inicializar se necessário
            if not mlflow_monitor._authenticated_tb:
                if not mlflow_monitor.initialize():
                    raise HTTPException(
                        status_code=500,
                        detail="Falha ao inicializar conexões (MLflow/ThingsBoard)"
                    )
            
            # Executar verificação
            results = mlflow_monitor.check_for_updates(experiment_names)
            
            success_count = sum(1 for r in results if r.get("success", False))
            failed_count = len(results) - success_count
            
            return SyncResultResponse(
                success=True,
                message=f"Sincronização completa: {success_count} sucesso, {failed_count} falhas",
                results=results,
                total_runs_processed=len(results),
                successful_syncs=success_count,
                failed_syncs=failed_count
            )
            
        except Exception as e:
            logger.error(f"Erro ao sincronizar: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    
    @router.get("/devices")
    async def list_devices():
        """
        Lista devices criados no ThingsBoard pelo monitor.
        """
        try:
            return {
                "success": True,
                "devices": [
                    {
                        "name": name,
                        "id": device["id"],
                        "token": device["token"][:10] + "..." if len(device["token"]) > 10 else device["token"]
                    }
                    for name, device in mlflow_monitor.device_cache.items()
                ],
                "total": len(mlflow_monitor.device_cache)
            }
            
        except Exception as e:
            logger.error(f"Erro ao listar devices: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    
    @router.delete("/cache")
    async def clear_cache():
        """
        Limpa o cache de devices e timestamps.
        """
        try:
            mlflow_monitor.device_cache.clear()
            mlflow_monitor.last_check_timestamps.clear()
            
            return {
                "success": True,
                "message": "Cache limpo com sucesso"
            }
            
        except Exception as e:
            logger.error(f"Erro ao limpar cache: {e}")
            raise HTTPException(status_code=500, detail=str(e))

except ImportError:
    # FastAPI não disponível (rodando como script standalone)
    router = None
    logger.debug("FastAPI não disponível - router não criado")
