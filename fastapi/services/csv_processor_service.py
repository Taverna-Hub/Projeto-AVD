"""
Serviço para processar arquivos CSV e enviar dados como telemetria.
Lê arquivos CSV das estações meteorológicas e converte em formato de telemetria.
"""

import os
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

from .thingsboard_service import ThingsBoardService

logger = logging.getLogger(__name__)


# Mapeamento de nomes de estações nos arquivos
ESTACAO_NOME_MAP = {
    "PETROLINA": "PETROLINA",
    "ARCO VERDE": "ARCO_VERDE",
    "CABROBO": "CABROBO",
    "CARUARU": "CARUARU",
    "FLORESTA": "FLORESTA",
    "GARANHUNS": "GARANHUNS",
    "IBIMIRIM": "IBIMIRIM",
    "OURICURI": "OURICURI",
    "PALMARES": "PALMARES",
    "SALGUEIRO": "SALGUEIRO",
    "SERRA TALHADA": "SERRA_TALHADA",
    "SURUBIM": "SURUBIM",
}


class CSVProcessorService:
    """Serviço para processar arquivos CSV e enviar telemetria."""
    
    def __init__(
        self,
        data_directory: str,
        tb_service: ThingsBoardService
    ):
        """
        Inicializa o serviço de processamento de CSV.
        
        Args:
            data_directory: Diretório com os arquivos CSV organizados por ano
            tb_service: Instância do ThingsBoardService
        """
        self.data_directory = Path(data_directory)
        self.tb_service = tb_service
        
    def encontrar_arquivos_estacao(
        self,
        nome_estacao: str,
        anos: Optional[List[str]] = None
    ) -> List[Path]:
        """
        Encontra todos os arquivos CSV de uma estação.
        
        Args:
            nome_estacao: Nome da estação (ex: "PETROLINA" ou "ARCO_VERDE")
            anos: Lista de anos para buscar (opcional, busca todos se None)
            
        Returns:
            Lista de caminhos dos arquivos encontrados
        """
        arquivos = []
        
        # Mapear nome normalizado para nome no arquivo
        # Arquivos têm espaços, mas devices usam underscore
        nome_arquivo = nome_estacao.replace("_", " ")
        
        # Se não especificado, buscar todos os anos disponíveis
        if anos is None:
            anos_dirs = [d for d in self.data_directory.iterdir() if d.is_dir()]
            anos = [d.name for d in anos_dirs if d.name.isdigit()]
        
        # Buscar arquivos em cada ano
        for ano in anos:
            ano_dir = self.data_directory / ano
            
            if not ano_dir.exists():
                logger.warning(f"Diretório do ano {ano} não encontrado")
                continue
            
            # Padrão: INMET_NE_PE_A###_ESTACAO_DD-MM-YYYY_A_DD-MM-YYYY.CSV
            pattern = f"*{nome_arquivo}*.CSV"
            
            arquivos_ano = list(ano_dir.glob(pattern))
            arquivos.extend(arquivos_ano)
            
            logger.debug(f"Encontrados {len(arquivos_ano)} arquivos para {nome_estacao} em {ano}")
        
        logger.info(f"Total de {len(arquivos)} arquivos encontrados para estação {nome_estacao}")
        
        return arquivos
    
    def processar_csv(
        self,
        arquivo_path: Path,
        encoding: str = 'latin-1'
    ) -> Optional[pd.DataFrame]:
        """
        Processa um arquivo CSV do INMET.
        
        Args:
            arquivo_path: Caminho do arquivo CSV
            encoding: Encoding do arquivo
            
        Returns:
            DataFrame processado ou None em caso de erro
        """
        try:
            # Ler arquivo pulando as linhas de cabeçalho do INMET
            df = pd.read_csv(
                arquivo_path,
                sep=';',
                encoding=encoding,
                skiprows=8,  # Pular cabeçalho INMET
                na_values=['-9999', '', ' ']
            )
            
            # Limpar nomes das colunas
            df.columns = df.columns.str.strip()
            
            # Verificar se tem coluna DATA e HORA
            if 'DATA' not in df.columns:
                logger.error(f"Coluna DATA não encontrada em {arquivo_path.name}")
                return None
            
            # Criar timestamp
            if 'HORA UTC' in df.columns:
                df['timestamp'] = pd.to_datetime(
                    df['DATA'] + ' ' + df['HORA UTC'].str.replace(' UTC', ''),
                    format='%Y/%m/%d %H%M',
                    errors='coerce'
                )
            else:
                df['timestamp'] = pd.to_datetime(df['DATA'], errors='coerce')
            
            # Remover linhas sem timestamp válido
            df = df.dropna(subset=['timestamp'])
            
            # Ordenar por timestamp
            df = df.sort_values('timestamp')
            
            logger.info(f"Processado {arquivo_path.name}: {len(df)} registros")
            
            return df
            
        except Exception as e:
            logger.error(f"Erro ao processar {arquivo_path.name}: {e}")
            return None
    
    def converter_para_telemetria(
        self,
        df: pd.DataFrame,
        colunas_telemetria: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Converte DataFrame em formato de telemetria para ThingsBoard.
        
        Args:
            df: DataFrame com dados processados
            colunas_telemetria: Lista de colunas para incluir (None = todas exceto timestamp)
            
        Returns:
            Lista de dicts com formato {ts: timestamp_ms, values: {key: value}}
        """
        telemetria = []
        
        # Se não especificado, usar todas as colunas exceto algumas
        colunas_ignorar = {'DATA', 'HORA UTC', 'timestamp'}
        
        if colunas_telemetria is None:
            colunas_telemetria = [col for col in df.columns if col not in colunas_ignorar]
        
        for _, row in df.iterrows():
            # Converter timestamp para milissegundos
            ts = int(row['timestamp'].timestamp() * 1000)
            
            # Extrair valores
            values = {}
            for col in colunas_telemetria:
                if col in row.index:
                    valor = row[col]
                    # Converter NaN para None
                    if pd.isna(valor):
                        values[col] = None
                    else:
                        values[col] = float(valor) if isinstance(valor, (int, float)) else str(valor)
            
            telemetria.append({
                "ts": ts,
                "values": values
            })
        
        return telemetria
    
    def enviar_telemetria_estacao(
        self,
        nome_estacao: str,
        device_token: str,
        anos: Optional[List[str]] = None,
        batch_size: int = 1000,
        colunas_telemetria: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Processa e envia telemetria de uma estação para ThingsBoard.
        
        Args:
            nome_estacao: Nome da estação
            device_token: Token do device no ThingsBoard
            anos: Lista de anos para processar (None = todos)
            batch_size: Tamanho do lote para envio
            colunas_telemetria: Colunas específicas para enviar
            
        Returns:
            Dict com estatísticas do processamento
        """
        logger.info(f"Iniciando processamento de telemetria para estação {nome_estacao}")
        
        # Encontrar arquivos
        arquivos = self.encontrar_arquivos_estacao(nome_estacao, anos)
        
        if not arquivos:
            logger.warning(f"Nenhum arquivo encontrado para estação {nome_estacao}")
            return {
                "estacao": nome_estacao,
                "arquivos_processados": 0,
                "registros_enviados": 0,
                "sucesso": 0,
                "falhas": 0
            }
        
        total_registros = 0
        total_sucesso = 0
        total_falhas = 0
        
        # Processar cada arquivo
        for arquivo in arquivos:
            logger.info(f"Processando arquivo {arquivo.name}")
            
            # Processar CSV
            df = self.processar_csv(arquivo)
            
            if df is None or df.empty:
                logger.warning(f"Arquivo {arquivo.name} vazio ou com erro")
                continue
            
            # Converter para telemetria
            telemetria = self.converter_para_telemetria(df, colunas_telemetria)
            
            if not telemetria:
                logger.warning(f"Nenhuma telemetria gerada do arquivo {arquivo.name}")
                continue
            
            # Enviar em lotes
            total_lotes = (len(telemetria) + batch_size - 1) // batch_size
            for i in range(0, len(telemetria), batch_size):
                lote = telemetria[i:i + batch_size]
                
                resultado = self.tb_service.send_batch_telemetry(device_token, lote)
                
                total_sucesso += resultado["success"]
                total_falhas += resultado["failed"]
                total_registros += len(lote)
                
                # Log a cada 5000 registros ou 20% do progresso
                lote_atual = (i // batch_size) + 1
                if total_registros % 5000 < batch_size or lote_atual % max(1, total_lotes // 5) == 0:
                    logger.info(
                        f"Progresso {nome_estacao}: {total_registros}/{len(telemetria)} "
                        f"registros enviados do arquivo {arquivo.name} "
                        f"({lote_atual}/{total_lotes} lotes)"
                    )
        
        resultado_final = {
            "estacao": nome_estacao,
            "arquivos_processados": len(arquivos),
            "registros_enviados": total_registros,
            "sucesso": total_sucesso,
            "falhas": total_falhas,
            "taxa_sucesso": (total_sucesso / total_registros * 100) if total_registros > 0 else 0
        }
        
        logger.info(
            f"Processamento concluído para {nome_estacao}: "
            f"{total_sucesso} sucessos, {total_falhas} falhas "
            f"de {total_registros} registros"
        )
        
        return resultado_final
    
    def enviar_telemetria_todas_estacoes(
        self,
        devices_info: List[Dict[str, str]],
        anos: Optional[List[str]] = None,
        batch_size: int = 100
    ) -> Dict[str, Any]:
        """
        Processa e envia telemetria de todas as estações.
        
        Args:
            devices_info: Lista de dicts com {nome: "ESTACAO", token: "TOKEN"}
            anos: Anos para processar
            batch_size: Tamanho do lote
            
        Returns:
            Dict com estatísticas gerais
        """
        logger.info(f"Iniciando processamento de telemetria para {len(devices_info)} estações")
        
        resultados_estacoes = []
        total_registros = 0
        total_sucesso = 0
        total_falhas = 0
        
        for device_info in devices_info:
            nome_estacao = device_info["nome"]
            token = device_info["token"]
            
            resultado = self.enviar_telemetria_estacao(
                nome_estacao=nome_estacao,
                device_token=token,
                anos=anos,
                batch_size=batch_size
            )
            
            resultados_estacoes.append(resultado)
            total_registros += resultado["registros_enviados"]
            total_sucesso += resultado["sucesso"]
            total_falhas += resultado["falhas"]
        
        resultado_geral = {
            "estacoes_processadas": len(devices_info),
            "total_registros": total_registros,
            "total_sucesso": total_sucesso,
            "total_falhas": total_falhas,
            "taxa_sucesso_geral": (total_sucesso / total_registros * 100) if total_registros > 0 else 0,
            "detalhes_estacoes": resultados_estacoes
        }
        
        logger.info(
            f"Processamento geral concluído: "
            f"{total_sucesso} sucessos, {total_falhas} falhas "
            f"de {total_registros} registros totais"
        )
        
        return resultado_geral


def create_csv_processor(
    data_directory: str,
    tb_service: ThingsBoardService
) -> CSVProcessorService:
    """
    Factory para criar instância do CSVProcessorService.
    
    Args:
        data_directory: Diretório com arquivos CSV
        tb_service: Instância do ThingsBoardService
        
    Returns:
        Instância do CSVProcessorService
    """
    return CSVProcessorService(data_directory, tb_service)
