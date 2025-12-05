"""
Script para enviar telemetrias dos CSVs do S3 para os devices processados no ThingsBoard.
Lê os CSVs de dados_imputados/resultados/ e envia para os devices "{CIDADE} - Processado".
"""
import os
import sys
import json
import requests
import boto3
import pandas as pd
import logging
import time
import socket
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from dotenv import load_dotenv

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Mapeamento de nomes de cidades (CSV -> Device)
STATION_MAPPING = {
    "ARCO_VERDE": "ARCO VERDE",
    "CABROBO": "CABROBO",
    "CARUARU": "CARUARU",
    "FLORESTA": "FLORESTA",
    "GARANHUNS": "GARANHUNS",
    "IBIMIRIM": "IBIMIRIM",
    "OURICURI": "OURICURI",
    "PALMARES": "PALMARES",
    "PETROLINA": "PETROLINA",
    "SALGUEIRO": "SALGUEIRO",
    "SERRA_TALHADA": "SERRA TALHADA",
    "SURUBIM": "SURUBIM"
}


class ThingsBoardClient:
    """Cliente para interagir com a API do ThingsBoard."""
    
    def __init__(self, host: str, port: int, username: str, password: str):
        self.base_url = f"http://{host}:{port}"
        self.token = None
        self.username = username
        self.password = password
        self.login()
    
    def login(self):
        """Realiza login no ThingsBoard e obtém o token JWT."""
        url = f"{self.base_url}/api/auth/login"
        payload = {"username": self.username, "password": self.password}
        
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
        return {
            "Content-Type": "application/json",
            "X-Authorization": f"Bearer {self.token}"
        }
    
    def get_device_by_name(self, name: str) -> Optional[Dict]:
        """Busca um device pelo nome exato."""
        url = f"{self.base_url}/api/tenant/devices?pageSize=100&page=0&textSearch={name}"
        
        try:
            response = requests.get(url, headers=self._get_headers())
            response.raise_for_status()
            data = response.json()
            devices = data.get('data', [])
            
            for device in devices:
                if device.get('name') == name:
                    return device
            return None
        except Exception as e:
            logger.error(f"Erro ao buscar device '{name}': {e}")
            return None
    
    def get_device_credentials(self, device_id: str) -> Optional[str]:
        """Obtém o access token do device."""
        url = f"{self.base_url}/api/device/{device_id}/credentials"
        
        try:
            response = requests.get(url, headers=self._get_headers())
            response.raise_for_status()
            credentials = response.json()
            return credentials.get("credentialsId")
        except Exception as e:
            logger.error(f"Erro ao obter credenciais do device {device_id}: {e}")
            return None
    
    def send_telemetry(self, access_token: str, telemetry_data: List[Dict], batch_size: int = 500) -> Dict:
        """
        Envia telemetria em lotes para um device.
        
        Args:
            access_token: Token de acesso do device
            telemetry_data: Lista de dados de telemetria com timestamp
            batch_size: Tamanho do lote
            
        Returns:
            Resultado do envio
        """
        url = f"{self.base_url}/api/v1/{access_token}/telemetry"
        
        success_count = 0
        failed_count = 0
        total = len(telemetry_data)
        
        # Enviar em lotes
        for i in range(0, total, batch_size):
            batch = telemetry_data[i:i + batch_size]
            
            try:
                response = requests.post(url, json=batch, timeout=30)
                response.raise_for_status()
                success_count += len(batch)
                
                if (i + batch_size) % 2000 == 0 or (i + batch_size) >= total:
                    logger.info(f"    Progresso: {min(i + batch_size, total)}/{total} registros enviados")
                    
            except Exception as e:
                logger.error(f"    Erro ao enviar lote {i//batch_size + 1}: {e}")
                failed_count += len(batch)
            
            # Pequeno delay para não sobrecarregar
            time.sleep(0.1)
        
        return {
            "success": success_count,
            "failed": failed_count,
            "total": total
        }


class S3DataLoader:
    """Classe para carregar dados do S3."""
    
    def __init__(self, bucket_name: str, access_key: str, secret_key: str, region: str):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        logger.info(f"✓ S3 Client inicializado para bucket: {bucket_name}")
    
    def list_csv_files(self, prefix: str = "dados_imputados/resultados/") -> List[str]:
        """Lista arquivos CSV no prefixo especificado."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            csv_files = [
                obj['Key'] for obj in response.get('Contents', [])
                if obj['Key'].endswith('.csv')
            ]
            
            return csv_files
        except Exception as e:
            logger.error(f"Erro ao listar arquivos do S3: {e}")
            return []
    
    def read_csv(self, s3_key: str) -> Optional[pd.DataFrame]:
        """Lê um arquivo CSV do S3."""
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            df = pd.read_csv(obj['Body'])
            return df
        except Exception as e:
            logger.error(f"Erro ao ler CSV {s3_key}: {e}")
            return None


def extract_station_from_filename(filename: str) -> Optional[str]:
    """
    Extrai o nome da estação do nome do arquivo.
    Ex: dados_para_update_neon_PETROLINA.csv -> PETROLINA
    Ex: dados_para_update_neon_ARCO_VERDE_Modelo_Leve.csv -> ARCO_VERDE
    """
    # Remover prefixo e extensão
    name = filename.replace("dados_para_update_neon_", "").replace(".csv", "")
    
    # Se tem _Modelo_, pegar parte antes
    if "_Modelo_" in name:
        name = name.split("_Modelo_")[0]
    
    return name


def prepare_telemetry_data(df: pd.DataFrame) -> List[Dict]:
    """
    Prepara dados de telemetria a partir do DataFrame.
    Converte data/hora para timestamp e formata os valores.
    """
    telemetry_data = []
    
    for _, row in df.iterrows():
        try:
            # Criar timestamp a partir de data e hora
            date_str = str(row['data'])
            hour = int(row['hora'])
            
            # Parsear data
            dt = datetime.strptime(f"{date_str} {hour:02d}:00:00", "%Y-%m-%d %H:%M:%S")
            timestamp = int(dt.timestamp() * 1000)  # ThingsBoard usa milissegundos
            
            # Criar objeto de telemetria
            telemetry = {
                "ts": timestamp,
                "values": {
                    "temperatura": float(row['temperatura']) if pd.notna(row['temperatura']) else None,
                    "umidade": float(row['umidade']) if pd.notna(row['umidade']) else None,
                    "velocidade_vento": float(row['velocidade_vento']) if pd.notna(row['velocidade_vento']) else None
                }
            }
            
            # Remover valores None
            telemetry["values"] = {k: v for k, v in telemetry["values"].items() if v is not None}
            
            if telemetry["values"]:
                telemetry_data.append(telemetry)
                
        except Exception as e:
            continue
    
    return telemetry_data


def send_data_to_devices(
    tb_client: ThingsBoardClient,
    s3_loader: S3DataLoader,
    use_base_csv_only: bool = True
) -> List[Dict]:
    """
    Envia dados dos CSVs do S3 para os devices do ThingsBoard.
    
    Args:
        tb_client: Cliente do ThingsBoard
        s3_loader: Loader do S3
        use_base_csv_only: Se True, usa apenas o CSV base (sem _Modelo_)
        
    Returns:
        Lista de resultados
    """
    results = []
    
    # Listar CSVs
    csv_files = s3_loader.list_csv_files()
    logger.info(f"\nEncontrados {len(csv_files)} arquivos CSV no S3")
    
    # Filtrar se necessário
    if use_base_csv_only:
        csv_files = [f for f in csv_files if "_Modelo_" not in f]
        logger.info(f"Usando apenas CSVs base (sem _Modelo_): {len(csv_files)} arquivos")
    
    for csv_file in csv_files:
        filename = csv_file.split("/")[-1]
        station_csv = extract_station_from_filename(filename)
        
        if not station_csv:
            logger.warning(f"Não foi possível extrair estação de: {filename}")
            continue
        
        # Mapear para nome do device
        station_name = STATION_MAPPING.get(station_csv)
        if not station_name:
            logger.warning(f"Estação {station_csv} não encontrada no mapeamento")
            continue
        
        device_name = f"{station_name} - Processado"
        logger.info(f"\n{'='*60}")
        logger.info(f"Processando: {filename}")
        logger.info(f"  Estação: {station_name}")
        logger.info(f"  Device: {device_name}")
        
        # Buscar device
        device = tb_client.get_device_by_name(device_name)
        if not device:
            logger.error(f"  ✗ Device não encontrado: {device_name}")
            results.append({
                "file": filename,
                "station": station_name,
                "device": device_name,
                "status": "error",
                "error": "Device não encontrado"
            })
            continue
        
        device_id = device.get('id', {}).get('id')
        access_token = tb_client.get_device_credentials(device_id)
        
        if not access_token:
            logger.error(f"  ✗ Não foi possível obter token do device")
            results.append({
                "file": filename,
                "station": station_name,
                "device": device_name,
                "status": "error",
                "error": "Falha ao obter token"
            })
            continue
        
        # Ler CSV
        logger.info(f"  Lendo CSV do S3...")
        df = s3_loader.read_csv(csv_file)
        
        if df is None or df.empty:
            logger.error(f"  ✗ CSV vazio ou erro ao ler")
            results.append({
                "file": filename,
                "station": station_name,
                "device": device_name,
                "status": "error",
                "error": "CSV vazio"
            })
            continue
        
        logger.info(f"  Registros no CSV: {len(df)}")
        
        # Preparar telemetria
        logger.info(f"  Preparando telemetria...")
        telemetry_data = prepare_telemetry_data(df)
        logger.info(f"  Registros de telemetria: {len(telemetry_data)}")
        
        # Enviar telemetria
        logger.info(f"  Enviando telemetria para ThingsBoard...")
        send_result = tb_client.send_telemetry(access_token, telemetry_data)
        
        status = "success" if send_result["success"] > 0 else "error"
        logger.info(f"  ✓ Enviados: {send_result['success']}/{send_result['total']} registros")
        
        results.append({
            "file": filename,
            "station": station_name,
            "device": device_name,
            "device_id": device_id,
            "status": status,
            "records_csv": len(df),
            "records_sent": send_result["success"],
            "records_failed": send_result["failed"]
        })
    
    return results


def get_thingsboard_host() -> str:
    """
    Detecta se está rodando dentro ou fora do Docker.
    Retorna o hostname apropriado para o ThingsBoard.
    """
    try:
        # Tenta resolver o hostname 'thingsboard' (usado dentro do Docker)
        socket.gethostbyname('thingsboard')
        return "thingsboard"
    except socket.gaierror:
        # Se falhar, está fora do Docker, usa localhost
        return "localhost"


def main():
    """Função principal do script."""
    
    # Carregar variáveis de ambiente
    load_dotenv()
    
    # Configurações do ThingsBoard (auto-detecta Docker ou localhost)
    TB_HOST = get_thingsboard_host()
    TB_PORT = int(os.getenv('TB_PORT', '9090'))
    TB_USERNAME = os.getenv('TB_USERNAME', 'tenant@thingsboard.org')
    TB_PASSWORD = os.getenv('TB_PASSWORD', 'tenant')
    
    # Configurações do S3
    S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'avd-5b')
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
    
    logger.info("\n" + "="*60)
    logger.info("ENVIO DE TELEMETRIAS PARA DEVICES PROCESSADOS")
    logger.info("="*60)
    logger.info(f"ThingsBoard: {TB_HOST}:{TB_PORT}")
    logger.info(f"S3 Bucket: {S3_BUCKET_NAME}")
    logger.info(f"Prefixo: dados_imputados/resultados/")
    logger.info("="*60)
    
    try:
        # Inicializar clientes
        logger.info("\nConectando ao ThingsBoard...")
        tb_client = ThingsBoardClient(TB_HOST, TB_PORT, TB_USERNAME, TB_PASSWORD)
        
        logger.info("Conectando ao S3...")
        s3_loader = S3DataLoader(
            S3_BUCKET_NAME,
            AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY,
            AWS_REGION
        )
        
        # Enviar dados
        results = send_data_to_devices(tb_client, s3_loader, use_base_csv_only=True)
        
        # Relatório final
        success_count = len([r for r in results if r.get('status') == 'success'])
        error_count = len([r for r in results if r.get('status') == 'error'])
        total_records = sum(r.get('records_sent', 0) for r in results)
        
        logger.info("\n" + "="*60)
        logger.info("RELATÓRIO FINAL")
        logger.info("="*60)
        logger.info(f"Devices processados com sucesso: {success_count}")
        logger.info(f"Devices com erro: {error_count}")
        logger.info(f"Total de registros enviados: {total_records}")
        
        logger.info("\nDetalhes por device:")
        for result in results:
            icon = "✓" if result.get('status') == 'success' else "✗"
            logger.info(f"  {icon} {result['device']}")
            if result.get('status') == 'success':
                logger.info(f"      Registros enviados: {result.get('records_sent', 0)}")
            else:
                logger.info(f"      Erro: {result.get('error', 'Desconhecido')}")
        
        # Salvar relatório
        report_path = Path(__file__).parent / 'telemetry_report.json'
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"\nRelatório salvo em: {report_path}")
        logger.info("="*60)
        logger.info("✓ Script concluído!")
        
    except Exception as e:
        logger.error(f"\n✗ Erro durante a execução: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
