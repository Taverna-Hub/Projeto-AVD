"""
Serviço para integração com Trendz Analytics.
Configura dashboards e análises de dados meteorológicos.
"""

import requests
import logging
from typing import Dict, List, Optional, Any
import pandas as pd

logger = logging.getLogger(__name__)


class TrendzService:
    """Serviço para integração com Trendz Analytics."""
    
    def __init__(
        self,
        trendz_url: str = "http://trendz:8888",
        tb_url: str = "http://thingsboard:9090"
    ):
        """
        Inicializa o serviço Trendz.
        
        Args:
            trendz_url: URL do Trendz Analytics
            tb_url: URL do ThingsBoard
        """
        self.trendz_url = trendz_url.rstrip('/')
        self.tb_url = tb_url.rstrip('/')
        self._authenticated = False
        self.jwt_token = None
        
    def authenticate(self, username: str, password: str) -> bool:
        """
        Autentica no Trendz Analytics.
        
        Args:
            username: Usuário
            password: Senha
            
        Returns:
            True se autenticado com sucesso
        """
        try:
            # Trendz usa autenticação do ThingsBoard
            url = f"{self.tb_url}/api/auth/login"
            payload = {
                "username": username,
                "password": password
            }
            
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            
            self.jwt_token = response.json().get("token")
            self._authenticated = True
            
            logger.info("Autenticado no Trendz com sucesso")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao autenticar no Trendz: {e}")
            self._authenticated = False
            return False
    
    def sync_data_sources(self) -> bool:
        """
        Sincroniza fontes de dados do ThingsBoard para o Trendz.
        
        Returns:
            True se sincronizado com sucesso
        """
        if not self._authenticated:
            logger.error("Não autenticado. Execute authenticate() primeiro")
            return False
        
        try:
            url = f"{self.trendz_url}/api/datasource/sync"
            headers = {
                "Authorization": f"Bearer {self.jwt_token}",
                "Content-Type": "application/json"
            }
            
            response = requests.post(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            logger.info("Fontes de dados sincronizadas com sucesso")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao sincronizar fontes de dados: {e}")
            return False
    
    def create_view(
        self,
        view_name: str,
        view_config: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Cria uma nova view no Trendz.
        
        Args:
            view_name: Nome da view
            view_config: Configuração da view
            
        Returns:
            Dict com informações da view criada
        """
        if not self._authenticated:
            logger.error("Não autenticado. Execute authenticate() primeiro")
            return None
        
        try:
            url = f"{self.trendz_url}/api/view"
            headers = {
                "Authorization": f"Bearer {self.jwt_token}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "name": view_name,
                "config": view_config
            }
            
            response = requests.post(url, json=payload, headers=headers, timeout=10)
            response.raise_for_status()
            
            view_info = response.json()
            logger.info(f"View '{view_name}' criada com sucesso")
            
            return view_info
            
        except Exception as e:
            logger.error(f"Erro ao criar view: {e}")
            return None
    
    def get_telemetry_stats(
        self,
        device_id: str,
        keys: List[str],
        start_ts: int,
        end_ts: int
    ) -> Optional[Dict[str, Any]]:
        """
        Obtém estatísticas de telemetria de um dispositivo.
        
        Args:
            device_id: ID do dispositivo
            keys: Lista de chaves de telemetria
            start_ts: Timestamp inicial (milissegundos)
            end_ts: Timestamp final (milissegundos)
            
        Returns:
            Dict com estatísticas
        """
        if not self._authenticated:
            logger.error("Não autenticado. Execute authenticate() primeiro")
            return None
        
        try:
            url = f"{self.trendz_url}/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries"
            headers = {
                "Authorization": f"Bearer {self.jwt_token}"
            }
            
            params = {
                "keys": ",".join(keys),
                "startTs": start_ts,
                "endTs": end_ts,
                "agg": "AVG,MIN,MAX,COUNT",
                "interval": 3600000  # 1 hora em milissegundos
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            stats = response.json()
            logger.info(f"Estatísticas obtidas para dispositivo {device_id}")
            
            return stats
            
        except Exception as e:
            logger.error(f"Erro ao obter estatísticas: {e}")
            return None
    
    def create_weather_dashboard(
        self,
        station_id: str,
        station_name: str
    ) -> Optional[str]:
        """
        Cria um dashboard padrão para estação meteorológica.
        
        Args:
            station_id: ID da estação no ThingsBoard
            station_name: Nome da estação
            
        Returns:
            ID do dashboard criado
        """
        if not self._authenticated:
            logger.error("Não autenticado. Execute authenticate() primeiro")
            return None
        
        try:
            # Configuração básica de dashboard para dados meteorológicos
            dashboard_config = {
                "title": f"Análise Meteorológica - {station_name}",
                "configuration": {
                    "timewindow": {
                        "realtime": {
                            "timewindowMs": 86400000  # 24 horas
                        }
                    },
                    "widgets": [
                        {
                            "type": "timeseries",
                            "title": "Temperatura ao Longo do Tempo",
                            "datasources": [
                                {
                                    "type": "device",
                                    "deviceId": station_id,
                                    "dataKeys": ["temperatura"]
                                }
                            ]
                        },
                        {
                            "type": "timeseries",
                            "title": "Umidade Relativa",
                            "datasources": [
                                {
                                    "type": "device",
                                    "deviceId": station_id,
                                    "dataKeys": ["umidade"]
                                }
                            ]
                        },
                        {
                            "type": "timeseries",
                            "title": "Radiação Solar",
                            "datasources": [
                                {
                                    "type": "device",
                                    "deviceId": station_id,
                                    "dataKeys": ["radiacao"]
                                }
                            ]
                        },
                        {
                            "type": "latest",
                            "title": "Últimas Medições",
                            "datasources": [
                                {
                                    "type": "device",
                                    "deviceId": station_id,
                                    "dataKeys": [
                                        "temperatura",
                                        "umidade",
                                        "vento_velocidade",
                                        "radiacao",
                                        "precipitacao"
                                    ]
                                }
                            ]
                        }
                    ]
                }
            }
            
            # Criar dashboard no ThingsBoard (Trendz usa dashboards do TB)
            url = f"{self.tb_url}/api/dashboard"
            headers = {
                "Authorization": f"Bearer {self.jwt_token}",
                "Content-Type": "application/json"
            }
            
            response = requests.post(url, json=dashboard_config, headers=headers, timeout=10)
            response.raise_for_status()
            
            dashboard_info = response.json()
            dashboard_id = dashboard_info.get("id", {}).get("id")
            
            logger.info(f"Dashboard criado com sucesso: {dashboard_id}")
            return dashboard_id
            
        except Exception as e:
            logger.error(f"Erro ao criar dashboard: {e}")
            return None


# Instância global do serviço
trendz_service = TrendzService()
