"""
Script para criar 12 devices no ThingsBoard e popular com dados dos arquivos CSV do S3.
Cada device corresponde a uma estação meteorológica com seus respectivos dados.
"""
import os
import sys
import time
import json
import re
import requests
import boto3
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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
            logger.info("Login realizado com sucesso no ThingsBoard")
        except Exception as e:
            logger.error(f"Erro ao fazer login no ThingsBoard: {e}")
            raise
    
    def _get_headers(self) -> Dict[str, str]:
        """Retorna os headers com o token de autenticação."""
        return {
            "Content-Type": "application/json",
            "X-Authorization": f"Bearer {self.token}"
        }
    
    def get_device_by_name(self, name: str) -> Optional[Dict]:
        """
        Busca um device pelo nome.
        
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
                    logger.info(f"Device '{name}' já existe - ID: {device.get('id', {}).get('id')}")
                    return device
            
            return None
        except Exception as e:
            logger.error(f"Erro ao buscar device '{name}': {e}")
            return None
    
    def delete_device(self, device_id: str) -> bool:
        """
        Deleta um device pelo ID.
        
        Args:
            device_id: ID do device
            
        Returns:
            True se deletado com sucesso
        """
        url = f"{self.base_url}/api/device/{device_id}"
        
        try:
            response = requests.delete(url, headers=self._get_headers())
            response.raise_for_status()
            logger.info(f"Device ID {device_id} deletado com sucesso")
            return True
        except Exception as e:
            logger.error(f"Erro ao deletar device {device_id}: {e}")
            return False
    
    def create_device(self, name: str, device_type: str = "weather_station") -> Dict:
        """
        Cria um novo device no ThingsBoard.
        
        Args:
            name: Nome do device
            device_type: Tipo do device
            
        Returns:
            Informações do device criado
        """
        url = f"{self.base_url}/api/device"
        payload = {
            "name": name,
            "type": device_type,
            "label": f"Estação Meteorológica - {name}"
        }
        
        try:
            response = requests.post(url, json=payload, headers=self._get_headers())
            response.raise_for_status()
            device = response.json()
            logger.info(f"Device '{name}' criado com sucesso - ID: {device.get('id', {}).get('id')}")
            return device
        except requests.exceptions.HTTPError as e:
            # Tentar obter mais detalhes do erro
            try:
                error_detail = e.response.json()
                logger.error(f"Erro ao criar device '{name}': {e.response.status_code} - {error_detail}")
            except:
                logger.error(f"Erro ao criar device '{name}': {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Erro ao criar device '{name}': {e}")
            raise
    
    def get_device_credentials(self, device_id: str) -> Dict:
        """
        Obtém as credenciais de acesso do device.
        
        Args:
            device_id: ID do device
            
        Returns:
            Credenciais do device
        """
        url = f"{self.base_url}/api/device/{device_id}/credentials"
        
        try:
            response = requests.get(url, headers=self._get_headers())
            response.raise_for_status()
            credentials = response.json()
            logger.info(f"Credenciais obtidas para device ID: {device_id}")
            return credentials
        except Exception as e:
            logger.error(f"Erro ao obter credenciais do device {device_id}: {e}")
            raise
    
    def send_telemetry(self, access_token: str, telemetry_data: Dict):
        """
        Envia dados de telemetria para um device.
        
        Args:
            access_token: Token de acesso do device
            telemetry_data: Dados de telemetria a serem enviados
        """
        url = f"{self.base_url}/api/v1/{access_token}/telemetry"
        
        try:
            response = requests.post(url, json=telemetry_data)
            response.raise_for_status()
            logger.debug(f"Telemetria enviada com sucesso")
        except Exception as e:
            logger.error(f"Erro ao enviar telemetria: {e}")
            raise
    
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
            logger.info(f"Atributos enviados com sucesso")
        except Exception as e:
            logger.error(f"Erro ao enviar atributos: {e}")
            raise


class S3DataLoader:
    """Classe para carregar dados do S3."""
    
    def __init__(self, bucket_name: str, access_key: str, secret_key: str, region: str):
        """
        Inicializa o loader do S3.
        
        Args:
            bucket_name: Nome do bucket S3
            access_key: AWS Access Key ID
            secret_key: AWS Secret Access Key
            region: Região AWS
        """
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        logger.info(f"S3 Client inicializado para bucket: {bucket_name}")
    
    def list_csv_files(self, prefix: str = "") -> List[str]:
        """
        Lista todos os arquivos CSV no bucket.
        
        Args:
            prefix: Prefixo para filtrar arquivos
            
        Returns:
            Lista de nomes de arquivos CSV
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            csv_files = [
                obj['Key'] for obj in response.get('Contents', [])
                if obj['Key'].lower().endswith('.csv')
            ]
            
            logger.info(f"Encontrados {len(csv_files)} arquivos CSV no bucket")
            return csv_files
        except Exception as e:
            logger.error(f"Erro ao listar arquivos do S3: {e}")
            raise
    
    def download_csv(self, s3_key: str, local_path: str):
        """
        Baixa um arquivo CSV do S3.
        
        Args:
            s3_key: Chave do arquivo no S3
            local_path: Caminho local para salvar o arquivo
        """
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            logger.info(f"Arquivo {s3_key} baixado para {local_path}")
        except Exception as e:
            logger.error(f"Erro ao baixar arquivo {s3_key}: {e}")
            raise
    
    def read_csv_from_s3(self, s3_key: str) -> pd.DataFrame:
        """
        Lê um arquivo CSV diretamente do S3.
        
        Args:
            s3_key: Chave do arquivo no S3
            
        Returns:
            DataFrame com os dados do CSV
        """
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            # Arquivos INMET têm metadados nas primeiras linhas, pular linhas iniciais
            df = pd.read_csv(obj['Body'], skiprows=8, sep=';', encoding='latin-1')
            logger.info(f"CSV {s3_key} lido com sucesso - {len(df)} linhas")
            return df
        except Exception as e:
            logger.error(f"Erro ao ler CSV {s3_key}: {e}")
            raise
    
    def read_csv_metadata(self, s3_key: str) -> Dict[str, str]:
        """
        Lê os metadados (primeiras 8 linhas) de um arquivo CSV do INMET.
        
        Args:
            s3_key: Chave do arquivo no S3
            
        Returns:
            Dicionário com os metadados extraídos
        """
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            
            # Ler as primeiras 8 linhas como texto
            content = obj['Body'].read().decode('latin-1')
            lines = content.split('\n')[:8]
            
            metadata = {}
            
            # Parsear cada linha de metadado
            for i, line in enumerate(lines, 1):
                if line.strip():
                    # Separar por ; e pegar chave-valor
                    parts = line.split(';')
                    if len(parts) >= 2:
                        key = parts[0].strip()
                        value = parts[1].strip() if len(parts) > 1 else ''
                        
                        # Criar chaves mais amigáveis
                        if 'REGIAO' in key.upper():
                            metadata['regiao'] = value
                        elif 'UF' in key.upper():
                            metadata['estado'] = value
                        elif 'ESTACAO' in key.upper() or 'ESTAÇÃO' in key.upper():
                            metadata['estacao'] = value
                        elif 'CODIGO' in key.upper() or 'CÓDIGO' in key.upper():
                            metadata['codigo_estacao'] = value
                        elif 'LATITUDE' in key.upper():
                            metadata['latitude'] = value
                        elif 'LONGITUDE' in key.upper():
                            metadata['longitude'] = value
                        elif 'ALTITUDE' in key.upper():
                            metadata['altitude'] = value
                        elif 'DATA DE FUNDACAO' in key.upper() or 'DATA DE FUNDAÇÃO' in key.upper():
                            metadata['data_fundacao'] = value
                        else:
                            # Adicionar outras informações com nome genérico
                            metadata[f'linha_{i}'] = f"{key}: {value}"
            
            logger.info(f"Metadados extraídos do CSV {s3_key}")
            return metadata
            
        except Exception as e:
            logger.error(f"Erro ao ler metadados do CSV {s3_key}: {e}")
            return {}


def process_csv_data(df: pd.DataFrame) -> List[Dict]:
    """
    Processa os dados do CSV para o formato do ThingsBoard.
    
    Args:
        df: DataFrame com os dados do CSV
        
    Returns:
        Lista de dicionários com telemetria formatada
    """
    telemetry_list = []
    
    # Mapeamento de colunas comuns de dados meteorológicos
    column_mapping = {
        'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)': 'temperature',
        'UMIDADE RELATIVA DO AR, HORARIA (%)': 'humidity',
        'VENTO, VELOCIDADE HORARIA (m/s)': 'wind_speed',
        'PRECIPITAÇÃO TOTAL, HORÁRIO (mm)': 'precipitation',
        'PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)': 'pressure',
        'RADIACAO GLOBAL (Kj/m²)': 'radiation',
        'Data': 'timestamp',
        'Hora UTC': 'time'
    }
    
    for _, row in df.iterrows():
        try:
            telemetry = {}
            timestamp = None
            
            # Processar cada coluna
            for col in df.columns:
                value = row[col]
                
                # Pular valores NaN
                if pd.isna(value):
                    continue
                
                # Tentar mapear para nome simplificado
                mapped_name = column_mapping.get(col, col.lower().replace(' ', '_'))
                
                # Lidar com timestamps
                if 'data' in col.lower() or 'timestamp' in mapped_name:
                    try:
                        # Tentar parsear a data
                        if isinstance(value, str):
                            timestamp = pd.to_datetime(value).timestamp() * 1000
                    except:
                        pass
                else:
                    # Tentar converter para número
                    try:
                        if isinstance(value, str):
                            value = value.replace(',', '.')
                        telemetry[mapped_name] = float(value)
                    except:
                        telemetry[mapped_name] = str(value)
            
            if telemetry:
                if timestamp:
                    telemetry_list.append({
                        'ts': int(timestamp),
                        'values': telemetry
                    })
                else:
                    telemetry_list.append(telemetry)
        
        except Exception as e:
            logger.warning(f"Erro ao processar linha: {e}")
            continue
    
    return telemetry_list


def group_csv_files_by_station(csv_files: List[str]) -> Dict[str, List[str]]:
    """
    Agrupa arquivos CSV por estação meteorológica.
    
    Args:
        csv_files: Lista de arquivos CSV
        
    Returns:
        Dicionário com código da estação como chave e lista de arquivos como valor
    """
    stations = {}
    
    for csv_file in csv_files:
        filename = Path(csv_file).stem
        parts = filename.split('_')
        
        # Extrair código da estação e nome da cidade
        station_code = None
        city_name = "Unknown"
        
        for i, part in enumerate(parts):
            if part.startswith('A') and len(part) == 4 and part[1:].isdigit():
                station_code = part
                if i + 1 < len(parts) and not parts[i + 1][0].isdigit():
                    city_name = parts[i + 1].title()
                break
        
        if station_code:
            if station_code not in stations:
                stations[station_code] = {
                    'city': city_name,
                    'files': []
                }
            stations[station_code]['files'].append(csv_file)
    
    return stations


def create_devices_from_csv(
    tb_client: ThingsBoardClient,
    s3_loader: S3DataLoader,
    csv_files: List[str],
    max_devices: int = 12
) -> List[Dict]:
    """
    Cria devices no ThingsBoard a partir dos arquivos CSV.
    Cada device representa uma estação e contém dados de todos os anos disponíveis.
    
    Args:
        tb_client: Cliente do ThingsBoard
        s3_loader: Loader do S3
        csv_files: Lista de arquivos CSV
        max_devices: Número máximo de devices a criar
        
    Returns:
        Lista de devices criados com suas informações
    """
    devices_created = []
    
    # Agrupar arquivos por estação meteorológica
    logger.info(f"\nAgrupando arquivos por estação meteorológica...")
    stations = group_csv_files_by_station(csv_files)
    logger.info(f"Encontradas {len(stations)} estações diferentes")
    
    # Limitar ao número máximo de devices
    station_codes = list(stations.keys())[:max_devices]
    
    for idx, station_code in enumerate(station_codes, 1):
        try:
            station_data = stations[station_code]
            device_name = station_data['city']
            station_files = sorted(station_data['files'])  # Ordenar arquivos por ano
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Processando Device {idx}/{len(station_codes)}: {device_name}")
            logger.info(f"Estação: {station_code}")
            logger.info(f"Arquivos encontrados: {len(station_files)}")
            for file in station_files:
                logger.info(f"  - {Path(file).name}")
            logger.info(f"{'='*60}")
            
            # Verificar se device já existe
            existing_device = tb_client.get_device_by_name(device_name)
            if existing_device:
                device_id = existing_device['id']['id']
                logger.info(f"Device já existe. Deletando para recriar com dados...")
                tb_client.delete_device(device_id)
                time.sleep(0.5)  # Pequeno delay após deletar
            
            # Criar device no ThingsBoard
            device = tb_client.create_device(device_name)
            device_id = device['id']['id']
            
            # Obter credenciais do device
            credentials = tb_client.get_device_credentials(device_id)
            access_token = credentials.get('credentialsId')
            
            # Processar todos os arquivos CSV desta estação
            total_records = 0
            all_years = []
            
            # Ler metadados do primeiro arquivo (contém info da estação)
            logger.info(f"Lendo metadados da estação...")
            metadata = s3_loader.read_csv_metadata(station_files[0])
            
            for year_idx, csv_file in enumerate(station_files, 1):
                try:
                    # Extrair ano do nome do arquivo
                    # Formato: INMET_NE_PE_A307_PETROLINA_01-01-2020_A_31-12-2020.CSV
                    year = None
                    filename = Path(csv_file).stem
                    # Procurar padrão de data DD-MM-YYYY
                    year_match = re.search(r'(\d{2}-\d{2}-(\d{4}))', filename)
                    if year_match:
                        year = year_match.group(2)
                    
                    logger.info(f"\n  Processando ano {year} ({year_idx}/{len(station_files)})...")
                    logger.info(f"  Arquivo: {Path(csv_file).name}")
                    
                    # Ler dados do CSV
                    df = s3_loader.read_csv_from_s3(csv_file)
                    logger.info(f"  {len(df)} registros lidos")
                    
                    # Processar e enviar telemetria
                    telemetry_list = process_csv_data(df)
                    
                    if telemetry_list:
                        logger.info(f"  Enviando {len(telemetry_list)} registros de telemetria...")
                        
                        # Enviar telemetria em lotes
                        batch_size = 100
                        for i in range(0, len(telemetry_list), batch_size):
                            batch = telemetry_list[i:i + batch_size]
                            
                            # Se a telemetria tem timestamp, enviar como lista
                            if 'ts' in batch[0]:
                                tb_client.send_telemetry(access_token, batch)
                            else:
                                # Senão, enviar registro por registro
                                for record in batch:
                                    tb_client.send_telemetry(access_token, record)
                            
                            # Pequeno delay para não sobrecarregar a API
                            time.sleep(0.1)
                            
                            if (i + batch_size) % 1000 == 0:
                                logger.info(f"    Progresso: {min(i + batch_size, len(telemetry_list))}/{len(telemetry_list)} registros")
                        
                        total_records += len(telemetry_list)
                        if year:
                            all_years.append(year)
                        logger.info(f"  ✓ Ano {year} processado com sucesso!")
                
                except Exception as e:
                    logger.error(f"  ✗ Erro ao processar arquivo {csv_file}: {e}")
                    continue
            
            # Preparar atributos do servidor com os metadados
            server_attributes = {
                'station_code': station_code,
                'csv_files': [Path(f).name for f in station_files],
                'years': ', '.join(sorted(all_years)),
                'total_years': len(all_years),
                'total_records': total_records,
                'created_at': datetime.now().isoformat()
            }
            
            # Adicionar metadados do INMET aos atributos do servidor
            server_attributes.update(metadata)
            
            logger.info(f"\nEnviando {len(server_attributes)} atributos do servidor...")
            tb_client.send_attributes(access_token, server_attributes)
            
            devices_created.append({
                'name': device_name,
                'id': device_id,
                'access_token': access_token,
                'station_code': station_code,
                'years': all_years,
                'csv_files': station_files,
                'records_sent': total_records
            })
            
            logger.info(f"✓ Device {device_name} criado e populado com sucesso!")
            
        except Exception as e:
            logger.error(f"✗ Erro ao processar device {idx}: {e}")
            continue
    
    return devices_created


def main():
    """Função principal do script."""
    
    # Carregar variáveis de ambiente
    from dotenv import load_dotenv
    load_dotenv()
    
    # Configurações do ThingsBoard
    TB_HOST = os.getenv('TB_HOST', 'localhost')
    TB_PORT = int(os.getenv('TB_PORT', '9090'))
    TB_USERNAME = os.getenv('TB_USERNAME', 'tenant@thingsboard.org')
    TB_PASSWORD = os.getenv('TB_PASSWORD', 'tenant')
    
    # Configurações do S3
    S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
    S3_PREFIX = os.getenv('S3_PREFIX', 'inmet-data')
    
    # Validar configurações
    if not all([S3_BUCKET_NAME, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY]):
        logger.error("Configurações do S3 não encontradas no .env")
        sys.exit(1)
    
    logger.info("\n" + "="*60)
    logger.info("SCRIPT DE CRIAÇÃO DE DEVICES NO THINGSBOARD")
    logger.info("="*60)
    logger.info(f"ThingsBoard: {TB_HOST}:{TB_PORT}")
    logger.info(f"S3 Bucket: {S3_BUCKET_NAME}")
    logger.info(f"S3 Prefix: {S3_PREFIX}")
    logger.info("="*60 + "\n")
    
    try:
        # Inicializar clientes
        logger.info("Inicializando ThingsBoard Client...")
        tb_client = ThingsBoardClient(TB_HOST, TB_PORT, TB_USERNAME, TB_PASSWORD)
        
        logger.info("Inicializando S3 Data Loader...")
        s3_loader = S3DataLoader(
            S3_BUCKET_NAME,
            AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY,
            AWS_REGION
        )
        
        # Listar arquivos CSV no S3
        logger.info(f"\nListando arquivos CSV no bucket (prefix: {S3_PREFIX})...")
        csv_files = s3_loader.list_csv_files(S3_PREFIX)
        
        if not csv_files:
            logger.error("Nenhum arquivo CSV encontrado no bucket!")
            sys.exit(1)
        
        logger.info(f"Encontrados {len(csv_files)} arquivos CSV")
        for i, file in enumerate(csv_files[:12], 1):
            logger.info(f"  {i}. {file}")
        
        # Criar devices
        logger.info(f"\nIniciando criação de até 12 devices...")
        devices = create_devices_from_csv(tb_client, s3_loader, csv_files, max_devices=12)
        
        # Relatório final
        logger.info("\n" + "="*60)
        logger.info("RELATÓRIO FINAL")
        logger.info("="*60)
        logger.info(f"Total de devices criados: {len(devices)}")
        
        for device in devices:
            logger.info(f"\nDevice: {device['name']}")
            logger.info(f"  ID: {device['id']}")
            logger.info(f"  Access Token: {device['access_token']}")
            logger.info(f"  Estação: {device['station_code']}")
            logger.info(f"  Anos: {', '.join(device['years'])}")
            logger.info(f"  Total de arquivos: {len(device['csv_files'])}")
            logger.info(f"  Registros enviados: {device['records_sent']}")
        
        # Salvar relatório em arquivo JSON
        report_path = Path(__file__).parent / 'devices_report.json'
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
