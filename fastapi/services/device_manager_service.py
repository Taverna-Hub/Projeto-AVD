"""
Serviço para gerenciar dispositivos no ThingsBoard.
Cria e configura devices para cada estação meteorológica.
"""

import logging
from typing import Dict, List, Optional, Any
from .thingsboard_service import ThingsBoardService

logger = logging.getLogger(__name__)


# Lista de estações meteorológicas de Pernambuco
# Nomes normalizados (com underscore) são convertidos para nomes com espaço ao criar devices
ESTACOES_METEOROLOGICAS = [
    {
        "nome": "PETROLINA",
        "codigo": "A307",
        "tipo": "Estação Meteorológica",
        "descricao": "Estação meteorológica do INMET em Petrolina - PE"
    },
    {
        "nome": "ARCO_VERDE",
        "codigo": "A309",
        "tipo": "Estação Meteorológica",
        "descricao": "Estação meteorológica do INMET em Arco Verde - PE"
    },
    {
        "nome": "CABROBO",
        "codigo": "A329",
        "tipo": "Estação Meteorológica",
        "descricao": "Estação meteorológica do INMET em Cabrobó - PE"
    },
    {
        "nome": "CARUARU",
        "codigo": "A341",
        "tipo": "Estação Meteorológica",
        "descricao": "Estação meteorológica do INMET em Caruaru - PE"
    },
    {
        "nome": "FLORESTA",
        "codigo": "A351",
        "tipo": "Estação Meteorológica",
        "descricao": "Estação meteorológica do INMET em Floresta - PE"
    },
    {
        "nome": "GARANHUNS",
        "codigo": "A322",
        "tipo": "Estação Meteorológica",
        "descricao": "Estação meteorológica do INMET em Garanhuns - PE"
    },
    {
        "nome": "IBIMIRIM",
        "codigo": "A349",
        "tipo": "Estação Meteorológica",
        "descricao": "Estação meteorológica do INMET em Ibimirim - PE"
    },
    {
        "nome": "OURICURI",
        "codigo": "A366",
        "tipo": "Estação Meteorológica",
        "descricao": "Estação meteorológica do INMET em Ouricuri - PE"
    },
    {
        "nome": "PALMARES",
        "codigo": "A357",
        "tipo": "Estação Meteorológica",
        "descricao": "Estação meteorológica do INMET em Palmares - PE"
    },
    {
        "nome": "SALGUEIRO",
        "codigo": "A370",
        "tipo": "Estação Meteorológica",
        "descricao": "Estação meteorológica do INMET em Salgueiro - PE"
    },
    {
        "nome": "SERRA_TALHADA",
        "codigo": "A350",
        "tipo": "Estação Meteorológica",
        "descricao": "Estação meteorológica do INMET em Serra Talhada - PE"
    },
    {
        "nome": "SURUBIM",
        "codigo": "A328",
        "tipo": "Estação Meteorológica",
        "descricao": "Estação meteorológica do INMET em Surubim - PE"
    },
]


class DeviceManagerService:
    """Serviço para gerenciar devices no ThingsBoard."""
    
    def __init__(self, thingsboard_service: ThingsBoardService):
        """
        Inicializa o serviço de gerenciamento de devices.
        
        Args:
            thingsboard_service: Instância do serviço ThingsBoard
        """
        self.tb_service = thingsboard_service
        self.devices_cache: Dict[str, Dict[str, Any]] = {}
        
    def criar_device_estacao(
        self,
        estacao: Dict[str, str],
        atributos: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Cria um device para uma estação meteorológica.
        
        Args:
            estacao: Dicionário com dados da estação (nome, código, tipo, descrição)
            atributos: Atributos adicionais do device (metadados de gráficos, etc.)
            
        Returns:
            Dict com informações do device criado incluindo token
        """
        # Nome do device é apenas o nome da cidade/estação
        device_name = estacao['nome'].replace('_', ' ').title()
        
        logger.info(f"Criando device para estação {estacao['nome']} ({estacao['codigo']})")
        
        # Criar device
        device_info = self.tb_service.create_device(
            device_name=device_name,
            device_type=estacao['tipo'],
            device_label=estacao['descricao']
        )
        
        if not device_info:
            logger.error(f"Falha ao criar device para estação {estacao['nome']}")
            return None
        
        device_id = device_info['id']['id']
        
        # Obter credenciais (token)
        token = self.tb_service.get_device_credentials(device_id)
        
        if not token:
            logger.error(f"Falha ao obter token para device {device_name}")
            return None
        
        device_info['token'] = token
        
        # Adicionar atributos padrão
        atributos_device = {
            "estacao": estacao['nome'],
            "codigo_wmo": estacao['codigo'],
            "regiao": "NE",
            "uf": "PE",
            "fonte": "INMET",
            "tipo": estacao['tipo']
        }
        
        # Adicionar atributos customizados
        if atributos:
            atributos_device.update(atributos)
        
        # Enviar atributos ao device
        if not self.tb_service.send_attributes(device_id, atributos_device):
            logger.warning(f"Falha ao enviar atributos para device {device_name}")
        
        # Cache do device
        self.devices_cache[estacao['nome']] = device_info
        
        logger.info(f"Device {device_name} criado com sucesso (ID: {device_id})")
        
        return device_info
    
    def criar_todos_devices(
        self,
        metadados_estacoes: Optional[Dict[str, Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Cria devices para todas as estações meteorológicas.
        
        Args:
            metadados_estacoes: Dict com metadados por estação (gráficos, modelos, etc.)
            
        Returns:
            Dict com resultados da criação (sucesso, falhas, devices)
        """
        resultados = {
            "sucesso": 0,
            "falhas": 0,
            "devices": []
        }
        
        logger.info(f"Iniciando criação de {len(ESTACOES_METEOROLOGICAS)} devices")
        
        for estacao in ESTACOES_METEOROLOGICAS:
            # Obter metadados específicos da estação se disponíveis
            atributos = None
            if metadados_estacoes and estacao['nome'] in metadados_estacoes:
                atributos = metadados_estacoes[estacao['nome']]
            
            device_info = self.criar_device_estacao(estacao, atributos)
            
            if device_info:
                resultados["sucesso"] += 1
                resultados["devices"].append({
                    "nome": estacao['nome'],
                    "device_id": device_info['id']['id'],
                    "token": device_info['token']
                })
            else:
                resultados["falhas"] += 1
        
        logger.info(
            f"Criação de devices concluída: "
            f"{resultados['sucesso']} sucessos, {resultados['falhas']} falhas"
        )
        
        return resultados
    
    def obter_device_por_estacao(self, nome_estacao: str) -> Optional[Dict[str, Any]]:
        """
        Obtém informações de um device pelo nome da estação.
        
        Args:
            nome_estacao: Nome da estação (ex: "PETROLINA")
            
        Returns:
            Dict com informações do device ou None
        """
        return self.devices_cache.get(nome_estacao)
    
    def listar_devices(self) -> List[Dict[str, Any]]:
        """
        Lista todos os devices criados.
        
        Returns:
            Lista com informações dos devices
        """
        return list(self.devices_cache.values())
    
    def adicionar_metadados_graficos(
        self,
        nome_estacao: str,
        metadados: Dict[str, Any]
    ) -> bool:
        """
        Adiciona metadados de gráficos aos atributos de um device.
        
        Args:
            nome_estacao: Nome da estação
            metadados: Dict com metadados dos gráficos (nome arquivo, tipo modelo, etc.)
            
        Returns:
            True se adicionado com sucesso
        """
        device_info = self.obter_device_por_estacao(nome_estacao)
        
        if not device_info:
            logger.error(f"Device não encontrado para estação {nome_estacao}")
            return False
        
        device_id = device_info['id']['id']
        
        # Preparar atributos de metadados
        atributos_metadados = {
            "graficos": metadados
        }
        
        # Enviar atributos
        if self.tb_service.send_attributes(device_id, atributos_metadados):
            logger.info(f"Metadados de gráficos adicionados ao device {nome_estacao}")
            return True
        else:
            logger.error(f"Falha ao adicionar metadados ao device {nome_estacao}")
            return False


def create_device_manager(tb_service: ThingsBoardService) -> DeviceManagerService:
    """
    Factory para criar instância do DeviceManagerService.
    
    Args:
        tb_service: Instância do ThingsBoardService
        
    Returns:
        Instância do DeviceManagerService
    """
    return DeviceManagerService(tb_service)
