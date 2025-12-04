"""
Service for uploading files to AWS S3.
Mantém a estrutura original dos arquivos CSV do INMET.
"""
import os
from pathlib import Path
from typing import Optional, Dict, List
import boto3
from botocore.exceptions import ClientError, BotoCoreError
import logging

logger = logging.getLogger(__name__)


class S3Service:
    """Service to upload files to AWS S3."""
    
    def __init__(
        self,
        bucket_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_region: str = "us-east-1",
        s3_prefix: Optional[str] = None,
    ):
        """
        Initialize S3Service.
        
        Args:
            bucket_name: Name of the S3 bucket
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            aws_region: AWS region
            s3_prefix: Optional prefix for organizing files in S3
        """
        self.bucket_name = bucket_name
        self.s3_prefix = s3_prefix or ""
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region,
        )
    
    def upload_file(
        self,
        local_file_path: str,
        s3_key: Optional[str] = None,
        preserve_structure: bool = True,
    ) -> Dict[str, str]:
        """
        Upload a single file to S3.
        
        Args:
            local_file_path: Path to the local file
            s3_key: Optional S3 key (object name). If not provided, uses relative path
            preserve_structure: If True, preserves directory structure in S3 key
            
        Returns:
            Dictionary with upload result:
            {
                "success": bool,
                "s3_key": str,
                "message": str
            }
        """
        file_path = Path(local_file_path)
        
        if not file_path.exists():
            return {
                "success": False,
                "s3_key": None,
                "message": f"File not found: {local_file_path}",
            }
        
        # Determine S3 key
        if s3_key is None:
            if preserve_structure:
                # Use relative path as S3 key
                s3_key = str(file_path.name)
            else:
                s3_key = file_path.name
        
        # Add prefix if specified
        if self.s3_prefix:
            s3_key = f"{self.s3_prefix}/{s3_key}".strip("/")
        
        try:
            # Upload file
            self.s3_client.upload_file(
                str(file_path),
                self.bucket_name,
                s3_key,
                ExtraArgs={"ContentType": "text/csv"},  # CSV files
            )
            
            logger.info(f"Successfully uploaded {local_file_path} to s3://{self.bucket_name}/{s3_key}")
            
            return {
                "success": True,
                "s3_key": s3_key,
                "message": f"File uploaded successfully to s3://{self.bucket_name}/{s3_key}",
            }
        
        except ClientError as e:
            error_msg = f"AWS ClientError uploading {local_file_path}: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "s3_key": s3_key,
                "message": error_msg,
            }
        
        except BotoCoreError as e:
            error_msg = f"AWS BotoCoreError uploading {local_file_path}: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "s3_key": s3_key,
                "message": error_msg,
            }
        
        except Exception as e:
            error_msg = f"Unexpected error uploading {local_file_path}: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "s3_key": s3_key,
                "message": error_msg,
            }
    
    def upload_file_with_structure(
        self,
        local_file_path: str,
        relative_path: str,
    ) -> Dict[str, str]:
        """
        Upload a file preserving the directory structure.
        
        Args:
            local_file_path: Path to the local file
            relative_path: Relative path from data directory (e.g., "2024/file.csv")
            
        Returns:
            Dictionary with upload result
        """
        # Use relative_path as S3 key to preserve structure
        s3_key = relative_path
        
        # Add prefix if specified
        if self.s3_prefix:
            s3_key = f"{self.s3_prefix}/{s3_key}".strip("/")
        
        return self.upload_file(local_file_path, s3_key=s3_key, preserve_structure=False)
    
    def check_bucket_exists(self) -> bool:
        """
        Check if the S3 bucket exists and is accessible.
        
        Returns:
            True if bucket exists and is accessible, False otherwise
        """
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            return True
        except ClientError:
            return False
    
    def upload_csv_estacao(
        self,
        arquivo_path: Path,
        ano: str
    ) -> Dict[str, any]:
        """
        Faz upload de um arquivo CSV de estação meteorológica mantendo estrutura original.
        Estrutura: data/ano/arquivo.CSV
        
        Args:
            arquivo_path: Caminho do arquivo CSV local
            ano: Ano dos dados (ex: 2020)
            
        Returns:
            Dicionário com resultado do upload
        """
        # Construir chave S3 mantendo a estrutura: data/ano/arquivo.CSV
        relative_path = f"{ano}/{arquivo_path.name}"
        
        return self.upload_file_with_structure(str(arquivo_path), relative_path)
    
    def upload_todos_csv_estacao(
        self,
        data_directory: Path,
        estacao_nome: str,
        anos: Optional[List[str]] = None
    ) -> Dict[str, any]:
        """
        Faz upload de todos os arquivos CSV de uma estação para S3.
        
        Args:
            data_directory: Diretório raiz com as pastas de anos
            estacao_nome: Nome da estação (ex: PETROLINA ou ARCO_VERDE)
            anos: Lista de anos específicos (None = todos os anos disponíveis)
            
        Returns:
            Dicionário com contadores de sucesso e falha
        """
        resultado = {
            'estacao': estacao_nome,
            'total': 0,
            'sucesso': 0,
            'falhas': 0,
            'arquivos': []
        }
        
        # Converter nome com underscore para espaço (como nos arquivos)
        nome_arquivo = estacao_nome.replace("_", " ")
        
        # Se não especificado, buscar todos os anos
        if anos is None:
            anos_dirs = [d for d in data_directory.iterdir() if d.is_dir() and d.name.isdigit()]
            anos = sorted([d.name for d in anos_dirs])
        
        logger.info(f"Iniciando upload para estação {estacao_nome} nos anos: {anos}")
        
        # Processar cada ano
        for ano in anos:
            ano_dir = data_directory / ano
            
            if not ano_dir.exists():
                logger.warning(f"Diretório do ano {ano} não encontrado")
                continue
            
            # Buscar arquivos CSV da estação
            pattern = f"*{nome_arquivo}*.CSV"
            arquivos = list(ano_dir.glob(pattern))
            
            logger.info(f"Encontrados {len(arquivos)} arquivos para {estacao_nome} em {ano}")
            
            # Upload de cada arquivo
            for arquivo in arquivos:
                resultado['total'] += 1
                
                upload_result = self.upload_csv_estacao(arquivo, ano)
                
                if upload_result['success']:
                    resultado['sucesso'] += 1
                    resultado['arquivos'].append({
                        'arquivo': arquivo.name,
                        'ano': ano,
                        's3_key': upload_result['s3_key'],
                        'status': 'sucesso'
                    })
                else:
                    resultado['falhas'] += 1
                    resultado['arquivos'].append({
                        'arquivo': arquivo.name,
                        'ano': ano,
                        'status': 'falha',
                        'erro': upload_result['message']
                    })
        
        logger.info(
            f"Upload concluído para {estacao_nome}: "
            f"{resultado['sucesso']}/{resultado['total']} arquivos enviados"
        )
        
        return resultado
    
    def upload_todas_estacoes(
        self,
        data_directory: Path,
        estacoes: List[Dict[str, str]],
        anos: Optional[List[str]] = None
    ) -> Dict[str, any]:
        """
        Faz upload de todos os arquivos CSV de todas as estações para S3.
        
        Args:
            data_directory: Diretório raiz com as pastas de anos
            estacoes: Lista de dicionários com informações das estações
            anos: Lista de anos específicos (None = todos os anos disponíveis)
            
        Returns:
            Dicionário com resumo geral e resultados por estação
        """
        resultado_geral = {
            'total_estacoes': len(estacoes),
            'total_arquivos': 0,
            'total_sucesso': 0,
            'total_falhas': 0,
            'estacoes_processadas': [],
            'detalhes': {}
        }
        
        logger.info(f"Iniciando upload de {len(estacoes)} estações para S3")
        
        for estacao in estacoes:
            nome = estacao['nome']
            logger.info(f"Processando estação: {nome}")
            
            resultado = self.upload_todos_csv_estacao(data_directory, nome, anos)
            
            resultado_geral['total_arquivos'] += resultado['total']
            resultado_geral['total_sucesso'] += resultado['sucesso']
            resultado_geral['total_falhas'] += resultado['falhas']
            resultado_geral['estacoes_processadas'].append(nome)
            resultado_geral['detalhes'][nome] = resultado
        
        logger.info(
            f"Upload geral concluído: {resultado_geral['total_sucesso']}/"
            f"{resultado_geral['total_arquivos']} arquivos enviados para S3"
        )
        
        return resultado_geral
    
    def listar_arquivos_bucket(self, prefix: Optional[str] = None) -> List[str]:
        """
        Lista arquivos no bucket S3.
        
        Args:
            prefix: Prefixo para filtrar arquivos (None = usar s3_prefix padrão)
            
        Returns:
            Lista de chaves dos arquivos no bucket
        """
        try:
            prefix_busca = prefix if prefix else self.s3_prefix
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix_busca
            )
            
            if 'Contents' not in response:
                logger.info(f"Nenhum arquivo encontrado com prefixo: {prefix_busca}")
                return []
            
            arquivos = [obj['Key'] for obj in response['Contents']]
            logger.info(f"Encontrados {len(arquivos)} arquivos no bucket")
            
            return arquivos
            
        except ClientError as e:
            logger.error(f"Erro ao listar arquivos do bucket: {str(e)}")
            return []


def create_s3_service(
    bucket_name: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    aws_region: str = "us-east-1",
    s3_prefix: str = "data"
) -> S3Service:
    """
    Factory function para criar instância do S3Service.
    
    Args:
        bucket_name: Nome do bucket S3
        aws_access_key_id: AWS Access Key ID
        aws_secret_access_key: AWS Secret Access Key
        aws_region: Região AWS
        s3_prefix: Prefixo dentro do bucket
        
    Returns:
        Instância configurada do S3Service
    """
    return S3Service(
        bucket_name=bucket_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_region=aws_region,
        s3_prefix=s3_prefix
    )
