"""
Script para criar 12 devices no ThingsBoard para dados PROCESSADOS.
Cada device corresponde a uma estação com formato: "{CIDADE} - Processado"
"""
import os
import sys
import json
import requests
import logging
from pathlib import Path
from typing import Dict, List, Optional
from dotenv import load_dotenv

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Lista das 12 cidades/estações
STATIONS = [
    "ARCO VERDE",
    "CABROBO",
    "CARUARU",
    "FLORESTA",
    "GARANHUNS",
    "IBIMIRIM",
    "OURICURI",
    "PALMARES",
    "PETROLINA",
    "SALGUEIRO",
    "SERRA TALHADA",
    "SURUBIM"
]


class ThingsBoardClient:
    """Cliente para interagir com a API do ThingsBoard."""
    
    def __init__(self, host: str, port: int, username: str, password: str):
        """
        Inicializa o cliente ThingsBoard.
        
        Args:
            host: Host do ThingsBoard
            port: Porta do ThingsBoard
            username: Usuário do tenant
            password: Senha do tenant
        """
        self.base_url = f"http://{host}:{port}"
        self.token = None
        self.username = username
        self.password = password
        self.login()
    
    def login(self):
        """Realiza login no ThingsBoard e obtém o token JWT."""
        url = f"{self.base_url}/api/auth/login"
        payload = {
            "username": self.username,
            "password": self.password
        }
        
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            data = response.json()
            self.token = data.get("token")
            logger.info("✓ Login realizado com sucesso no ThingsBoard")
        except Exception as e:
            logger.error(f"✗ Erro ao fazer login no ThingsBoard: {e}")
            raise
    
    def _get_headers(self) -> Dict[str, str]:
        """Retorna os headers com o token de autenticação."""
        return {
            "Content-Type": "application/json",
            "X-Authorization": f"Bearer {self.token}"
        }
    
    def get_device_by_name(self, name: str) -> Optional[Dict]:
        """
        Busca um device pelo nome exato.
        
        Args:
            name: Nome do device
            
        Returns:
            Device encontrado ou None
        """
        url = f"{self.base_url}/api/tenant/devices?pageSize=100&page=0&textSearch={name}"
        
        try:
            response = requests.get(url, headers=self._get_headers())
            response.raise_for_status()
            data = response.json()
            devices = data.get('data', [])
            
            # Procurar device com nome exato
            for device in devices:
                if device.get('name') == name:
                    return device
            
            return None
        except Exception as e:
            logger.error(f"Erro ao buscar device '{name}': {e}")
            return None
    
    def create_device(self, name: str, device_type: str = "weather_station_processed", label: str = None) -> Optional[Dict]:
        """
        Cria um novo device no ThingsBoard.
        
        Args:
            name: Nome do device
            device_type: Tipo do device
            label: Label do device
            
        Returns:
            Informações do device criado ou None se falhar
        """
        url = f"{self.base_url}/api/device"
        payload = {
            "name": name,
            "type": device_type,
            "label": label or f"Dados Processados - {name}"
        }
        
        try:
            response = requests.post(url, json=payload, headers=self._get_headers())
            response.raise_for_status()
            device = response.json()
            return device
        except requests.exceptions.HTTPError as e:
            try:
                error_detail = e.response.json()
                logger.error(f"Erro ao criar device '{name}': {e.response.status_code} - {error_detail}")
            except:
                logger.error(f"Erro ao criar device '{name}': {e.response.status_code} - {e.response.text}")
            return None
        except Exception as e:
            logger.error(f"Erro ao criar device '{name}': {e}")
            return None
    
    def get_device_credentials(self, device_id: str) -> Optional[str]:
        """
        Obtém o access token do device.
        
        Args:
            device_id: ID do device
            
        Returns:
            Access token do device
        """
        url = f"{self.base_url}/api/device/{device_id}/credentials"
        
        try:
            response = requests.get(url, headers=self._get_headers())
            response.raise_for_status()
            credentials = response.json()
            return credentials.get("credentialsId")
        except Exception as e:
            logger.error(f"Erro ao obter credenciais do device {device_id}: {e}")
            return None
    
    def send_attributes(self, access_token: str, attributes: Dict):
        """
        Envia atributos para um device.
        
        Args:
            access_token: Token de acesso do device
            attributes: Atributos a serem enviados
        """
        url = f"{self.base_url}/api/v1/{access_token}/attributes"
        
        try:
            response = requests.post(url, json=attributes)
            response.raise_for_status()
            logger.debug(f"Atributos enviados com sucesso")
        except Exception as e:
            logger.error(f"Erro ao enviar atributos: {e}")


def create_processed_devices(tb_client: ThingsBoardClient) -> List[Dict]:
    """
    Cria os 12 devices para dados processados.
    
    Args:
        tb_client: Cliente do ThingsBoard
        
    Returns:
        Lista de devices criados
    """
    devices_created = []
    devices_existing = []
    
    for station in STATIONS:
        device_name = f"{station} - Processado"
        
        logger.info(f"\nProcessando: {device_name}")
        
        # Verificar se device já existe
        existing_device = tb_client.get_device_by_name(device_name)
        
        if existing_device:
            device_id = existing_device.get('id', {}).get('id')
            access_token = tb_client.get_device_credentials(device_id)
            
            logger.info(f"  ↳ Device já existe - ID: {device_id}")
            devices_existing.append({
                'name': device_name,
                'id': device_id,
                'access_token': access_token,
                'station': station,
                'status': 'existing'
            })
            continue
        
        # Criar novo device
        device = tb_client.create_device(
            name=device_name,
            device_type="weather_station_processed",
            label=f"Estação Meteorológica Processada - {station}"
        )
        
        if not device:
            logger.error(f"  ✗ Falha ao criar device: {device_name}")
            continue
        
        device_id = device.get('id', {}).get('id')
        access_token = tb_client.get_device_credentials(device_id)
        
        # Enviar atributos iniciais
        attributes = {
            "station_name": station,
            "data_type": "processed",
            "description": f"Dados meteorológicos processados da estação {station}",
            "created_by": "create_processed_devices.py"
        }
        
        if access_token:
            tb_client.send_attributes(access_token, attributes)
        
        logger.info(f"  ✓ Device criado - ID: {device_id}")
        
        devices_created.append({
            'name': device_name,
            'id': device_id,
            'access_token': access_token,
            'station': station,
            'status': 'created'
        })
    
    return devices_created + devices_existing


def main():
    """Função principal do script."""
    
    # Carregar variáveis de ambiente
    load_dotenv()
    
    # Configurações do ThingsBoard
    TB_HOST = os.getenv('TB_HOST', 'localhost')
    TB_PORT = int(os.getenv('TB_PORT', '9090'))
    TB_USERNAME = os.getenv('TB_USERNAME', 'tenant@thingsboard.org')
    TB_PASSWORD = os.getenv('TB_PASSWORD', 'tenant')
    
    logger.info("\n" + "="*60)
    logger.info("CRIAÇÃO DE DEVICES PARA DADOS PROCESSADOS")
    logger.info("="*60)
    logger.info(f"ThingsBoard: {TB_HOST}:{TB_PORT}")
    logger.info(f"Total de estações: {len(STATIONS)}")
    logger.info("="*60)
    
    logger.info("\nEstações a serem criadas:")
    for i, station in enumerate(STATIONS, 1):
        logger.info(f"  {i:2d}. {station} - Processado")
    
    try:
        # Inicializar cliente
        logger.info("\nConectando ao ThingsBoard...")
        tb_client = ThingsBoardClient(TB_HOST, TB_PORT, TB_USERNAME, TB_PASSWORD)
        
        # Criar devices
        logger.info("\nCriando devices...")
        devices = create_processed_devices(tb_client)
        
        # Relatório final
        created = [d for d in devices if d.get('status') == 'created']
        existing = [d for d in devices if d.get('status') == 'existing']
        
        logger.info("\n" + "="*60)
        logger.info("RELATÓRIO FINAL")
        logger.info("="*60)
        logger.info(f"Devices criados: {len(created)}")
        logger.info(f"Devices já existentes: {len(existing)}")
        logger.info(f"Total: {len(devices)}")
        
        logger.info("\nLista de devices:")
        for device in devices:
            status_icon = "✓" if device['status'] == 'created' else "○"
            logger.info(f"  {status_icon} {device['name']}")
            logger.info(f"      ID: {device['id']}")
            logger.info(f"      Token: {device['access_token']}")
        
        # Salvar relatório em arquivo JSON
        report_path = Path(__file__).parent / 'processed_devices_report.json'
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(devices, f, indent=2, ensure_ascii=False)
        
        logger.info(f"\nRelatório salvo em: {report_path}")
        logger.info("="*60)
        logger.info("✓ Script concluído com sucesso!")
        
    except Exception as e:
        logger.error(f"\n✗ Erro durante a execução do script: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
