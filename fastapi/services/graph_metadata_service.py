"""
Serviço para extrair metadados de gráficos dos notebooks.
Identifica arquivos CSV gerados pelos notebooks e extrai informações.
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


# Tipos de modelos identificados nos arquivos
TIPOS_MODELO = {
    "Leve": {
        "descricao": "Modelo leve de previsão de sensação térmica",
        "complexidade": "baixa"
    },
    "Medio": {
        "descricao": "Modelo médio de previsão de sensação térmica",
        "complexidade": "media"
    },
    "Robusto": {
        "descricao": "Modelo robusto de previsão de sensação térmica",
        "complexidade": "alta"
    }
}


class GraphMetadataService:
    """Serviço para extrair metadados de gráficos dos notebooks."""
    
    def __init__(self, notebooks_directory: str):
        """
        Inicializa o serviço de metadados.
        
        Args:
            notebooks_directory: Diretório com notebooks e arquivos CSV gerados
        """
        self.notebooks_dir = Path(notebooks_directory)
        
    def extrair_metadados_arquivo(self, arquivo_path: Path) -> Optional[Dict[str, Any]]:
        """
        Extrai metadados de um arquivo CSV gerado pelos notebooks.
        
        Args:
            arquivo_path: Caminho do arquivo CSV
            
        Returns:
            Dict com metadados extraídos
        """
        nome_arquivo = arquivo_path.name
        
        # Padrão: dados_para_update_neon_ESTACAO_Modelo_Tipo.csv
        # ou: dados_para_update_neon_ESTACAO.csv
        
        metadados = {
            "nome_arquivo": nome_arquivo,
            "caminho_completo": str(arquivo_path),
            "tamanho_bytes": arquivo_path.stat().st_size if arquivo_path.exists() else 0
        }
        
        # Extrair informações do nome do arquivo
        if "dados_para_update_neon" in nome_arquivo:
            partes = nome_arquivo.replace("dados_para_update_neon_", "").replace(".csv", "").split("_")
            
            if len(partes) >= 1:
                # Primeira parte é sempre a estação
                estacao = partes[0]
                metadados["estacao"] = estacao
                metadados["fonte"] = "Dados processados para update Neon"
                
                # Verificar se tem informação de modelo
                if len(partes) >= 3 and partes[1] == "Modelo":
                    tipo_modelo = partes[2]
                    metadados["tipo_modelo"] = tipo_modelo
                    
                    if tipo_modelo in TIPOS_MODELO:
                        metadados["descricao_modelo"] = TIPOS_MODELO[tipo_modelo]["descricao"]
                        metadados["complexidade_modelo"] = TIPOS_MODELO[tipo_modelo]["complexidade"]
                else:
                    metadados["tipo_dataset"] = "Dados gerais da estação"
        
        return metadados
    
    def listar_arquivos_graficos(
        self,
        nome_estacao: Optional[str] = None
    ) -> List[Path]:
        """
        Lista arquivos CSV relacionados a gráficos.
        
        Args:
            nome_estacao: Filtrar por estação específica (opcional)
            
        Returns:
            Lista de caminhos dos arquivos
        """
        # Buscar arquivos CSV no diretório de notebooks
        pattern = "dados_para_update_neon*.csv"
        
        if nome_estacao:
            pattern = f"dados_para_update_neon_{nome_estacao}*.csv"
        
        arquivos = list(self.notebooks_dir.glob(pattern))
        
        logger.info(f"Encontrados {len(arquivos)} arquivos de gráficos")
        
        return sorted(arquivos)
    
    def extrair_metadados_estacao(
        self,
        nome_estacao: str
    ) -> Dict[str, Any]:
        """
        Extrai todos os metadados relacionados a uma estação.
        
        Args:
            nome_estacao: Nome da estação
            
        Returns:
            Dict com todos os metadados organizados
        """
        logger.info(f"Extraindo metadados para estação {nome_estacao}")
        
        arquivos = self.listar_arquivos_graficos(nome_estacao)
        
        metadados_estacao = {
            "estacao": nome_estacao,
            "total_arquivos": len(arquivos),
            "arquivos_dados": [],
            "modelos": {}
        }
        
        for arquivo in arquivos:
            metadados = self.extrair_metadados_arquivo(arquivo)
            
            if metadados:
                metadados_estacao["arquivos_dados"].append(metadados)
                
                # Organizar por tipo de modelo
                if "tipo_modelo" in metadados:
                    tipo = metadados["tipo_modelo"]
                    if tipo not in metadados_estacao["modelos"]:
                        metadados_estacao["modelos"][tipo] = []
                    metadados_estacao["modelos"][tipo].append({
                        "arquivo": metadados["nome_arquivo"],
                        "descricao": metadados.get("descricao_modelo", ""),
                        "complexidade": metadados.get("complexidade_modelo", "")
                    })
        
        return metadados_estacao
    
    def extrair_metadados_todas_estacoes(self) -> Dict[str, Dict[str, Any]]:
        """
        Extrai metadados de todas as estações encontradas.
        
        Returns:
            Dict com metadados por estação {ESTACAO: metadados}
        """
        logger.info("Extraindo metadados de todas as estações")
        
        # Buscar todos os arquivos
        arquivos = self.listar_arquivos_graficos()
        
        # Extrair estações únicas
        estacoes = set()
        for arquivo in arquivos:
            metadados = self.extrair_metadados_arquivo(arquivo)
            if metadados and "estacao" in metadados:
                estacoes.add(metadados["estacao"])
        
        # Extrair metadados completos por estação
        metadados_completos = {}
        for estacao in estacoes:
            metadados_completos[estacao] = self.extrair_metadados_estacao(estacao)
        
        logger.info(f"Metadados extraídos para {len(metadados_completos)} estações")
        
        return metadados_completos
    
    def formatar_para_atributos_device(
        self,
        metadados_estacao: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Formata metadados no formato adequado para atributos do device.
        
        Args:
            metadados_estacao: Metadados extraídos da estação
            
        Returns:
            Dict formatado para envio como atributos
        """
        atributos = {
            "graficos_total": metadados_estacao["total_arquivos"],
            "graficos_arquivos": [
                {
                    "nome": arq["nome_arquivo"],
                    "tipo": arq.get("tipo_modelo", "dados_gerais"),
                    "tamanho_mb": round(arq["tamanho_bytes"] / (1024 * 1024), 2)
                }
                for arq in metadados_estacao["arquivos_dados"]
            ]
        }
        
        # Adicionar informações de modelos
        if metadados_estacao["modelos"]:
            atributos["modelos_disponiveis"] = list(metadados_estacao["modelos"].keys())
            atributos["modelos_detalhes"] = metadados_estacao["modelos"]
        
        return atributos
    
    def gerar_resumo_metadados(self) -> Dict[str, Any]:
        """
        Gera um resumo geral dos metadados disponíveis.
        
        Returns:
            Dict com estatísticas gerais
        """
        metadados_todas = self.extrair_metadados_todas_estacoes()
        
        total_arquivos = sum(m["total_arquivos"] for m in metadados_todas.values())
        total_modelos = sum(len(m["modelos"]) for m in metadados_todas.values())
        
        estacoes_com_modelos = [
            est for est, m in metadados_todas.items()
            if m["modelos"]
        ]
        
        resumo = {
            "total_estacoes": len(metadados_todas),
            "total_arquivos_graficos": total_arquivos,
            "total_tipos_modelos": total_modelos,
            "estacoes_com_modelos": estacoes_com_modelos,
            "estacoes_processadas": list(metadados_todas.keys())
        }
        
        logger.info(
            f"Resumo: {resumo['total_estacoes']} estações, "
            f"{resumo['total_arquivos_graficos']} arquivos, "
            f"{len(resumo['estacoes_com_modelos'])} estações com modelos"
        )
        
        return resumo


def create_graph_metadata_service(notebooks_directory: str) -> GraphMetadataService:
    """
    Factory para criar instância do GraphMetadataService.
    
    Args:
        notebooks_directory: Diretório com notebooks
        
    Returns:
        Instância do GraphMetadataService
    """
    return GraphMetadataService(notebooks_directory)
