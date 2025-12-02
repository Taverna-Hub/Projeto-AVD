"""
Serviço para integração com ThingsBoard.
Envia telemetria de dados meteorológicos tratados.
"""

import requests
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)


class ThingsBoardService:
    """Serviço para enviar dados ao ThingsBoard."""
    
    def __init__(
        self,
        tb_url: str = "http://thingsboard:9090",
        access_token: Optional[str] = None
    ):
        """
        Inicializa o serviço ThingsBoard.
        
        Args:
            tb_url: URL do ThingsBoard
            access_token: Token de acesso do dispositivo
        """
        self.tb_url = tb_url.rstrip('/')
        self.access_token = access_token
        self._authenticated = False
        self.jwt_token = None
        
    def authenticate(self, username: str, password: str) -> bool:
        """
        Autentica no ThingsBoard e obtém JWT token.
        
        Args:
            username: Usuário do ThingsBoard
            password: Senha do ThingsBoard
            
        Returns:
            True se autenticado com sucesso
        """
        try:
            url = f"{self.tb_url}/api/auth/login"
            payload = {
                "username": username,
                "password": password
            }
            
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            
            self.jwt_token = response.json().get("token")
            self._authenticated = True
            
            logger.info("Autenticado no ThingsBoard com sucesso")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao autenticar no ThingsBoard: {e}")
            self._authenticated = False
            return False
    
    def send_telemetry(
        self,
        device_token: str,
        telemetry_data: Dict[str, Any],
        timestamp: Optional[int] = None
    ) -> bool:
        """
        Envia telemetria para um dispositivo específico.
        
        Args:
            device_token: Token de acesso do dispositivo
            telemetry_data: Dados de telemetria (dict com chave-valor)
            timestamp: Timestamp em milissegundos (opcional)
            
        Returns:
            True se enviado com sucesso
        """
        try:
            url = f"{self.tb_url}/api/v1/{device_token}/telemetry"
            
            if timestamp:
                payload = {
                    "ts": timestamp,
                    "values": telemetry_data
                }
            else:
                payload = telemetry_data
            
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            
            return True
            
        except Exception as e:
            logger.error(f"Erro ao enviar telemetria: {e}")
            return False
    
    def send_batch_telemetry(
        self,
        device_token: str,
        telemetry_list: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """
        Envia múltiplas entradas de telemetria em lote.
        
        Args:
            device_token: Token de acesso do dispositivo
            telemetry_list: Lista de dicts com 'ts' e 'values'
            
        Returns:
            Dict com contadores de sucesso/falha
        """
        success_count = 0
        failed_count = 0
        
        for telemetry in telemetry_list:
            timestamp = telemetry.get('ts')
            values = telemetry.get('values', telemetry)
            
            if self.send_telemetry(device_token, values, timestamp):
                success_count += 1
            else:
                failed_count += 1
        
        logger.info(
            f"Batch telemetry enviada: {success_count} sucesso, "
            f"{failed_count} falhas"
        )
        
        return {
            "success": success_count,
            "failed": failed_count,
            "total": len(telemetry_list)
        }
    
    def send_dataframe(
        self,
        device_token: str,
        df: pd.DataFrame,
        timestamp_column: Optional[str] = None,
        batch_size: int = 100
    ) -> Dict[str, int]:
        """
        Envia dados de um DataFrame para ThingsBoard.
        
        Args:
            device_token: Token de acesso do dispositivo
            df: DataFrame com os dados
            timestamp_column: Nome da coluna com timestamps
            batch_size: Tamanho do lote para envio
            
        Returns:
            Dict com estatísticas do envio
        """
        total_rows = len(df)
        success_count = 0
        failed_count = 0
        
        logger.info(f"Enviando {total_rows} registros para ThingsBoard")
        
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i + batch_size]
            telemetry_list = []
            
            for idx, row in batch.iterrows():
                # Converter para timestamp em milissegundos
                if timestamp_column and timestamp_column in df.columns:
                    ts = int(pd.Timestamp(row[timestamp_column]).timestamp() * 1000)
                elif isinstance(idx, pd.Timestamp):
                    ts = int(idx.timestamp() * 1000)
                else:
                    ts = int(datetime.now().timestamp() * 1000)
                
                # Remover coluna de timestamp dos valores
                values = row.to_dict()
                if timestamp_column and timestamp_column in values:
                    del values[timestamp_column]
                
                # Converter valores NaN para None
                values = {k: (None if pd.isna(v) else v) for k, v in values.items()}
                
                telemetry_list.append({
                    "ts": ts,
                    "values": values
                })
            
            # Enviar lote
            result = self.send_batch_telemetry(device_token, telemetry_list)
            success_count += result["success"]
            failed_count += result["failed"]
            
            if (i + batch_size) % 1000 == 0:
                logger.info(f"Progresso: {i + batch_size}/{total_rows} registros processados")
        
        logger.info(
            f"Envio concluído: {success_count} sucesso, "
            f"{failed_count} falhas de {total_rows} total"
        )
        
        return {
            "success": success_count,
            "failed": failed_count,
            "total": total_rows,
            "success_rate": (success_count / total_rows * 100) if total_rows > 0 else 0
        }
    
    def create_device(
        self,
        device_name: str,
        device_type: str = "sensor",
        device_label: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Cria um novo dispositivo no ThingsBoard.
        
        Args:
            device_name: Nome do dispositivo
            device_type: Tipo do dispositivo
            device_label: Label do dispositivo (opcional)
            
        Returns:
            Dict com informações do dispositivo criado
        """
        if not self._authenticated:
            logger.error("Não autenticado. Execute authenticate() primeiro")
            return None
        
        try:
            url = f"{self.tb_url}/api/device"
            headers = {
                "Authorization": f"Bearer {self.jwt_token}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "name": device_name,
                "type": device_type
            }
            
            if device_label:
                payload["label"] = device_label
            
            response = requests.post(url, json=payload, headers=headers, timeout=10)
            response.raise_for_status()
            
            device_info = response.json()
            logger.info(f"Dispositivo '{device_name}' criado com sucesso")
            
            return device_info
            
        except Exception as e:
            logger.error(f"Erro ao criar dispositivo: {e}")
            return None
    
    def get_device_credentials(self, device_id: str) -> Optional[str]:
        """
        Obtém o token de acesso de um dispositivo.
        
        Args:
            device_id: ID do dispositivo
            
        Returns:
            Token de acesso do dispositivo
        """
        if not self._authenticated:
            logger.error("Não autenticado. Execute authenticate() primeiro")
            return None
        
        try:
            url = f"{self.tb_url}/api/device/{device_id}/credentials"
            headers = {
                "Authorization": f"Bearer {self.jwt_token}"
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            credentials = response.json()
            token = credentials.get("credentialsId")
            
            logger.info(f"Token obtido para dispositivo {device_id}")
            return token
            
        except Exception as e:
            logger.error(f"Erro ao obter credenciais do dispositivo: {e}")
            return None


# Instância global do serviço
thingsboard_service = ThingsBoardService()
