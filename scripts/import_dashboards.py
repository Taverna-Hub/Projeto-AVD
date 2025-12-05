"""
Script para importar dashboards do ThingsBoard a partir de arquivos JSON.
Lê os arquivos JSON da pasta reports/graficos_thingsboard e cria/atualiza os dashboards.
"""

import os
import sys
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
import requests

# Adicionar o diretório raiz ao path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente
env_path = project_root / ".env"
if env_path.exists():
    load_dotenv(env_path)


class ThingsBoardDashboardImporter:
    """Importador de dashboards para ThingsBoard."""
    
    def __init__(
        self,
        tb_url: str,
        username: str,
        password: str
    ):
        """
        Inicializa o importador.
        
        Args:
            tb_url: URL do ThingsBoard
            username: Usuário do ThingsBoard
            password: Senha do ThingsBoard
        """
        self.tb_url = tb_url.rstrip('/')
        self.username = username
        self.password = password
        self.jwt_token = None
        self.session = requests.Session()
        
    def authenticate(self) -> bool:
        """
        Autentica no ThingsBoard.
        
        Returns:
            True se autenticado com sucesso
        """
        try:
            url = f"{self.tb_url}/api/auth/login"
            payload = {
                "username": self.username,
                "password": self.password
            }
            
            response = self.session.post(url, json=payload, timeout=10)
            response.raise_for_status()
            
            self.jwt_token = response.json().get("token")
            self.session.headers.update({
                "Authorization": f"Bearer {self.jwt_token}",
                "Content-Type": "application/json"
            })
            
            logger.info("✓ Autenticado no ThingsBoard com sucesso")
            return True
            
        except Exception as e:
            logger.error(f"✗ Erro ao autenticar no ThingsBoard: {e}")
            return False
    
    def get_dashboard_by_title(self, title: str) -> Optional[Dict[str, Any]]:
        """
        Busca um dashboard pelo título.
        
        Args:
            title: Título do dashboard
            
        Returns:
            Dashboard encontrado ou None
        """
        try:
            url = f"{self.tb_url}/api/tenant/dashboards"
            params = {
                "pageSize": 1000,
                "page": 0,
                "textSearch": title
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            dashboards = response.json().get("data", [])
            
            # Procurar dashboard exato pelo título
            for dashboard in dashboards:
                if dashboard.get("title") == title:
                    return dashboard
            
            return None
            
        except Exception as e:
            logger.error(f"Erro ao buscar dashboard '{title}': {e}")
            return None
    
    def create_dashboard(self, dashboard_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Cria um novo dashboard.
        
        Args:
            dashboard_data: Dados do dashboard
            
        Returns:
            Dashboard criado ou None
        """
        try:
            url = f"{self.tb_url}/api/dashboard"
            
            response = self.session.post(url, json=dashboard_data, timeout=30)
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Erro ao criar dashboard: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Resposta: {e.response.text}")
            return None
    
    def update_dashboard(
        self,
        dashboard_id: str,
        dashboard_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Atualiza um dashboard existente.
        
        Args:
            dashboard_id: ID do dashboard
            dashboard_data: Dados atualizados do dashboard
            
        Returns:
            Dashboard atualizado ou None
        """
        try:
            url = f"{self.tb_url}/api/dashboard"
            
            # Adicionar ID ao payload
            dashboard_data["id"] = {"id": dashboard_id, "entityType": "DASHBOARD"}
            
            response = self.session.post(url, json=dashboard_data, timeout=30)
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Erro ao atualizar dashboard: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Resposta: {e.response.text}")
            return None
    
    def import_dashboard_from_file(
        self,
        file_path: Path,
        update_if_exists: bool = True
    ) -> bool:
        """
        Importa um dashboard de um arquivo JSON.
        
        Args:
            file_path: Caminho do arquivo JSON
            update_if_exists: Se True, atualiza dashboard se já existir
            
        Returns:
            True se importado com sucesso
        """
        try:
            # Ler arquivo JSON
            with open(file_path, 'r', encoding='utf-8') as f:
                dashboard_json = json.load(f)
            
            title = dashboard_json.get("title", file_path.stem)
            
            # Verificar se dashboard já existe
            existing_dashboard = self.get_dashboard_by_title(title)
            
            if existing_dashboard:
                if update_if_exists:
                    logger.info(f"Dashboard '{title}' já existe, atualizando...")
                    result = self.update_dashboard(
                        existing_dashboard["id"]["id"],
                        dashboard_json
                    )
                    if result:
                        logger.info(f"✓ Dashboard '{title}' atualizado com sucesso")
                        return True
                    else:
                        logger.error(f"✗ Falha ao atualizar dashboard '{title}'")
                        return False
                else:
                    logger.info(f"⊘ Dashboard '{title}' já existe, pulando")
                    return True
            else:
                logger.info(f"Criando novo dashboard '{title}'...")
                result = self.create_dashboard(dashboard_json)
                if result:
                    logger.info(f"✓ Dashboard '{title}' criado com sucesso")
                    return True
                else:
                    logger.error(f"✗ Falha ao criar dashboard '{title}'")
                    return False
                    
        except json.JSONDecodeError as e:
            logger.error(f"✗ Erro ao ler JSON do arquivo {file_path.name}: {e}")
            return False
        except Exception as e:
            logger.error(f"✗ Erro ao importar dashboard de {file_path.name}: {e}")
            return False
    
    def import_all_dashboards(
        self,
        dashboards_dir: Path,
        update_if_exists: bool = True
    ) -> Dict[str, int]:
        """
        Importa todos os dashboards de um diretório.
        
        Args:
            dashboards_dir: Diretório com os arquivos JSON
            update_if_exists: Se True, atualiza dashboards que já existem
            
        Returns:
            Dicionário com contadores de sucesso e falha
        """
        resultado = {
            'total': 0,
            'sucesso': 0,
            'falhas': 0,
            'pulados': 0,
            'dashboards': []
        }
        
        # Buscar todos os arquivos JSON recursivamente
        json_files = list(dashboards_dir.rglob("*.json"))
        
        if not json_files:
            logger.warning(f"Nenhum arquivo JSON encontrado em {dashboards_dir}")
            return resultado
        
        logger.info(f"Encontrados {len(json_files)} arquivos JSON")
        
        for json_file in json_files:
            resultado['total'] += 1
            logger.info(f"\n{'='*60}")
            logger.info(f"Processando: {json_file.relative_to(dashboards_dir)}")
            logger.info(f"{'='*60}")
            
            if self.import_dashboard_from_file(json_file, update_if_exists):
                resultado['sucesso'] += 1
                resultado['dashboards'].append({
                    'arquivo': json_file.name,
                    'pasta': json_file.parent.name,
                    'status': 'sucesso'
                })
            else:
                resultado['falhas'] += 1
                resultado['dashboards'].append({
                    'arquivo': json_file.name,
                    'pasta': json_file.parent.name,
                    'status': 'falha'
                })
        
        return resultado


def main():
    """Função principal."""
    print("\n" + "="*60)
    print("IMPORTADOR DE DASHBOARDS DO THINGSBOARD")
    print("="*60)
    
    # Verificar variáveis de ambiente (suporta ambos os nomes)
    tb_url = os.getenv("THINGSBOARD_URL", os.getenv("TB_URL", "http://localhost:9090"))
    username = os.getenv("TB_USERNAME", os.getenv("THINGSBOARD_USERNAME"))
    password = os.getenv("TB_PASSWORD", os.getenv("THINGSBOARD_PASSWORD"))
    
    if not username or not password:
        logger.error("✗ Variáveis TB_USERNAME e TB_PASSWORD não configuradas")
        logger.info("Configure no arquivo .env:")
        logger.info("  TB_USERNAME=seu_usuario")
        logger.info("  TB_PASSWORD=sua_senha")
        return
    
    # Diretório com os dashboards
    dashboards_dir = project_root / "reports" / "graficos_thingsboard"
    
    if not dashboards_dir.exists():
        logger.error(f"✗ Diretório não encontrado: {dashboards_dir}")
        return
    
    logger.info(f"Diretório de dashboards: {dashboards_dir}")
    logger.info(f"ThingsBoard URL: {tb_url}")
    logger.info(f"Usuário: {username}")
    
    # Criar importador
    importer = ThingsBoardDashboardImporter(tb_url, username, password)
    
    # Autenticar
    if not importer.authenticate():
        logger.error("✗ Falha na autenticação")
        return
    
    # Importar todos os dashboards
    print("\n" + "="*60)
    print("IMPORTANDO DASHBOARDS")
    print("="*60)
    
    resultado = importer.import_all_dashboards(
        dashboards_dir=dashboards_dir,
        update_if_exists=True
    )
    
    # Resumo
    print("\n" + "="*60)
    print("RESUMO DA IMPORTAÇÃO")
    print("="*60)
    print(f"Total de arquivos: {resultado['total']}")
    print(f"✓ Sucesso: {resultado['sucesso']}")
    print(f"✗ Falhas: {resultado['falhas']}")
    
    if resultado['sucesso'] > 0:
        taxa = (resultado['sucesso'] / resultado['total'] * 100)
        print(f"Taxa de sucesso: {taxa:.2f}%")
    
    # Detalhes
    if resultado['dashboards']:
        print("\nDetalhes:")
        for dash in resultado['dashboards']:
            status_icon = "✓" if dash['status'] == 'sucesso' else "✗"
            print(f"  {status_icon} {dash['pasta']}/{dash['arquivo']}")
    
    print("\n" + "="*60)
    print("IMPORTAÇÃO CONCLUÍDA")
    print("="*60)


if __name__ == "__main__":
    main()
