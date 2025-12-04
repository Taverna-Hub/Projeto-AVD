"""
Script para criar dashboards automaticamente no ThingsBoard.
Versão simplificada e funcional.
"""

import os
import sys
import requests
from typing import Dict, List, Optional
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ThingsBoardDashboardCreator:
    """Criador automático de dashboards no ThingsBoard."""
    
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.token = None
        self.devices = []
        
    def authenticate(self) -> bool:
        """Autentica no ThingsBoard."""
        try:
            response = requests.post(
                f"{self.base_url}/api/auth/login",
                json={"username": self.username, "password": self.password},
                timeout=10
            )
            response.raise_for_status()
            self.token = response.json()["token"]
            logger.info("✅ Autenticado com sucesso")
            return True
        except Exception as e:
            logger.error(f"❌ Erro ao autenticar: {e}")
            return False
    
    def _get_headers(self) -> Dict[str, str]:
        """Retorna headers com token de autenticação."""
        return {
            "Content-Type": "application/json",
            "X-Authorization": f"Bearer {self.token}"
        }
    
    def get_devices(self, name_filter: str = "Processado") -> List[Dict]:
        """Busca devices do tenant."""
        try:
            response = requests.get(
                f"{self.base_url}/api/tenant/devices",
                params={"pageSize": 1000, "page": 0},
                headers=self._get_headers(),
                timeout=10
            )
            response.raise_for_status()
            devices = response.json()["data"]
            
            if name_filter:
                devices = [d for d in devices if name_filter in d["name"]]
            
            self.devices = devices
            logger.info(f"✅ Encontrados {len(devices)} devices")
            return devices
        except Exception as e:
            logger.error(f"❌ Erro ao buscar devices: {e}")
            return []
    
    def create_simple_dashboard(self, title: str, device_ids: List[str] = None) -> Optional[str]:
        """
        Cria um dashboard simples e funcional.
        
        Args:
            title: Título do dashboard
            device_ids: Lista de IDs dos devices (opcional)
        
        Returns:
            ID do dashboard criado ou None
        """
        try:
            # Configuração básica do dashboard
            dashboard_config = {
                "title": title,
                "configuration": {
                    "description": "",
                    "widgets": {},
                    "states": {
                        "default": {
                            "name": "State",
                            "root": True,
                            "layouts": {
                                "main": {
                                    "widgets": {},
                                    "gridSettings": {
                                        "backgroundColor": "#eeeeee",
                                        "columns": 24,
                                        "margin": 10,
                                        "outerMargin": True,
                                        "backgroundSizeMode": "100%"
                                    }
                                }
                            }
                        }
                    },
                    "entityAliases": {},
                    "filters": {},
                    "timewindow": {
                        "hideInterval": False,
                        "hideLastInterval": False,
                        "hideQuickInterval": False,
                        "hideAggregation": False,
                        "hideAggInterval": False,
                        "hideTimezone": False,
                        "selectedTab": 0,
                        "realtime": {
                            "realtimeType": 1,
                            "interval": 1000,
                            "timewindowMs": 604800000,
                            "quickInterval": "CURRENT_DAY"
                        },
                        "history": {
                            "historyType": 0,
                            "interval": 1000,
                            "timewindowMs": 60000,
                            "fixedTimewindow": {
                                "startTimeMs": 0,
                                "endTimeMs": 0
                            },
                            "quickInterval": "CURRENT_DAY"
                        },
                        "aggregation": {
                            "type": "AVG",
                            "limit": 25000
                        }
                    },
                    "settings": {
                        "stateControllerId": "entity",
                        "showTitle": False,
                        "showDashboardsSelect": True,
                        "showEntitiesSelect": True,
                        "showDashboardTimewindow": True,
                        "showDashboardExport": True,
                        "toolbarAlwaysOpen": True
                    }
                }
            }
            
            # Criar dashboard vazio
            response = requests.post(
                f"{self.base_url}/api/dashboard",
                json=dashboard_config,
                headers=self._get_headers(),
                timeout=10
            )
            response.raise_for_status()
            dashboard = response.json()
            dashboard_id = dashboard["id"]["id"]
            
            logger.info(f"✅ Dashboard '{title}' criado")
            logger.info(f"   URL: {self.base_url}/dashboard/{dashboard_id}")
            logger.info(f"   ⚠️ ATENÇÃO: Adicione widgets manualmente na UI do ThingsBoard")
            
            return dashboard_id
            
        except Exception as e:
            logger.error(f"❌ Erro ao criar dashboard '{title}': {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"   Response: {e.response.text}")
            return None
    
    def create_all_dashboards(self):
        """Cria dashboards básicos."""
        logger.info("=" * 70)
        logger.info("CRIAÇÃO DE DASHBOARDS NO THINGSBOARD")
        logger.info("=" * 70)
        
        if not self.authenticate():
            return False
        
        if not self.get_devices():
            logger.error("❌ Nenhum device encontrado")
            return False
        
        # Dashboard principal
        logger.info("\n📊 Criando Dashboard Principal...")
        main_id = self.create_simple_dashboard(
            "🌤️ Estações Meteorológicas - Visão Geral"
        )
        
        # Dashboards individuais
        logger.info("\n📊 Criando Dashboards Individuais...")
        logger.info(f"Total de estações: {len(self.devices)}")
        created = 0
        for device in self.devices:  # Criar para TODAS as estações
            station_name = device["name"].replace(" - Processado", "")
            dashboard_id = self.create_simple_dashboard(
                f"📍 {station_name}"
            )
            if dashboard_id:
                created += 1
        
        # Instruções finais
        logger.info("\n" + "=" * 70)
        logger.info("✅ DASHBOARDS CRIADOS COM SUCESSO")
        logger.info("=" * 70)
        logger.info(f"Total: {created + (1 if main_id else 0)} dashboards")
        logger.info(f"\n📊 Acesse: {self.base_url}/dashboards")
        logger.info("\n⚠️ PRÓXIMOS PASSOS:")
        logger.info("1. Acesse cada dashboard no ThingsBoard")
        logger.info("2. Clique em 'Edit mode' (ícone de lápis)")
        logger.info("3. Clique em '+' para adicionar widget")
        logger.info("4. Escolha 'Charts' > 'Timeseries - Flot'")
        logger.info("5. Configure:")
        logger.info("   - Entity alias: Crie com filtro 'Entity name' contendo 'Processado'")
        logger.info("   - Data keys: temperatura, umidade, velocidade_vento")
        logger.info("   - Timewindow: Last 7 days")
        logger.info("6. Salve o widget e o dashboard")
        logger.info("=" * 70)
        
        return True


def main():
    """Função principal."""
    from dotenv import load_dotenv
    load_dotenv()
    
    # Configurações
    TB_HOST = os.getenv('TB_HOST', 'thingsboard')
    if TB_HOST == 'thingsboard' and not os.path.exists('/.dockerenv'):
        TB_HOST = 'localhost'
    
    TB_PORT = os.getenv('TB_PORT', '9090')
    TB_BASE_URL = f"http://{TB_HOST}:{TB_PORT}"
    TB_USERNAME = os.getenv('TB_USERNAME', 'tenant@thingsboard.org')
    TB_PASSWORD = os.getenv('TB_PASSWORD', 'tenant')
    
    logger.info(f"ThingsBoard URL: {TB_BASE_URL}")
    
    creator = ThingsBoardDashboardCreator(TB_BASE_URL, TB_USERNAME, TB_PASSWORD)
    success = creator.create_all_dashboards()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
