"""
Utilitários para conexão e operações com Neon PostgreSQL.

Este módulo fornece funções auxiliares para conectar e manipular dados
no banco PostgreSQL Neon a partir de notebooks ou scripts.
"""

import os
from pathlib import Path
from typing import Optional, Dict, Any, List
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
import psycopg2
from dotenv import load_dotenv


class NeonConnection:
    """Gerenciador de conexão com Neon PostgreSQL."""
    
    def __init__(self, env_path: Optional[str] = None):
        """
        Inicializa conexão com Neon.
        
        Args:
            env_path: Caminho para arquivo .env (padrão: ../.env)
        """
        # Carrega variáveis de ambiente
        if env_path is None:
            env_path = Path(__file__).parent.parent / '.env'
        else:
            env_path = Path(env_path)
            
        if env_path.exists():
            load_dotenv(env_path)
        
        # Configurações
        self.config = {
            'user': os.getenv('NEON_USER'),
            'password': os.getenv('NEON_PASSWORD'),
            'host': os.getenv('NEON_HOST'),
            'database': os.getenv('NEON_DB'),
            'port': os.getenv('NEON_PORT', '5432')
        }
        
        # Valida configurações
        missing = [k for k, v in self.config.items() if not v]
        if missing:
            raise ValueError(f"Variáveis de ambiente faltando: {', '.join(missing)}")
        
        self._engine: Optional[Engine] = None
    
    @property
    def engine(self) -> Engine:
        """Retorna engine SQLAlchemy (cria se necessário)."""
        if self._engine is None:
            self._engine = self._create_engine()
        return self._engine
    
    def _create_engine(self) -> Engine:
        """Cria engine SQLAlchemy."""
        database_url = (
            f"postgresql://{self.config['user']}:{self.config['password']}"
            f"@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            f"?sslmode=require"
        )
        
        return create_engine(
            database_url,
            echo=False,
            pool_pre_ping=True,
            connect_args={
                'connect_timeout': 10,
                'sslmode': 'require'
            }
        )
    
    def test_connection(self) -> bool:
        """
        Testa conexão com Neon.
        
        Returns:
            True se conexão bem-sucedida, False caso contrário
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
            return True
        except Exception as e:
            print(f"Erro na conexão: {e}")
            return False
    
    def get_tables(self, schema: str = 'public') -> List[str]:
        """
        Lista tabelas disponíveis.
        
        Args:
            schema: Schema do banco (padrão: 'public')
            
        Returns:
            Lista de nomes de tabelas
        """
        inspector = inspect(self.engine)
        return inspector.get_table_names(schema=schema)
    
    def table_exists(self, table_name: str, schema: str = 'public') -> bool:
        """
        Verifica se tabela existe.
        
        Args:
            table_name: Nome da tabela
            schema: Schema do banco
            
        Returns:
            True se tabela existe
        """
        return table_name in self.get_tables(schema)
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Executa query SQL e retorna DataFrame.
        
        Args:
            query: Query SQL
            
        Returns:
            DataFrame com resultados
        """
        return pd.read_sql(query, self.engine)
    
    def save_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = 'append',
        index: bool = False,
        chunksize: int = 1000
    ) -> int:
        """
        Salva DataFrame no banco.
        
        Args:
            df: DataFrame para salvar
            table_name: Nome da tabela
            if_exists: 'fail', 'replace', ou 'append'
            index: Se True, salva índice como coluna
            chunksize: Tamanho do lote para inserção
            
        Returns:
            Número de linhas salvas
        """
        df.to_sql(
            table_name,
            self.engine,
            if_exists=if_exists,
            index=index,
            method='multi',
            chunksize=chunksize
        )
        return len(df)
    
    def get_table_info(self, table_name: str, schema: str = 'public') -> Dict[str, Any]:
        """
        Obtém informações sobre uma tabela.
        
        Args:
            table_name: Nome da tabela
            schema: Schema do banco
            
        Returns:
            Dict com informações da tabela
        """
        if not self.table_exists(table_name, schema):
            raise ValueError(f"Tabela '{table_name}' não existe no schema '{schema}'")
        
        inspector = inspect(self.engine)
        columns = inspector.get_columns(table_name, schema=schema)
        
        # Conta registros
        with self.engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name}"))
            count = result.fetchone()[0]
        
        return {
            'table_name': table_name,
            'schema': schema,
            'columns': [col['name'] for col in columns],
            'column_types': {col['name']: str(col['type']) for col in columns},
            'row_count': count
        }
    
    def create_weather_table(self, table_name: str = 'weather_data') -> None:
        """
        Cria tabela para dados meteorológicos.
        
        Args:
            table_name: Nome da tabela
        """
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            estacao VARCHAR(50) NOT NULL,
            temperatura FLOAT,
            umidade FLOAT,
            vento_velocidade FLOAT,
            radiacao FLOAT,
            precipitacao FLOAT,
            temp_media_3h FLOAT,
            temp_diff FLOAT,
            hora_dia INTEGER,
            radiacao_log FLOAT,
            data_processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(timestamp, estacao)
        );
        
        CREATE INDEX IF NOT EXISTS idx_timestamp ON {table_name}(timestamp);
        CREATE INDEX IF NOT EXISTS idx_estacao ON {table_name}(estacao);
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        
        print(f"✅ Tabela '{table_name}' criada com sucesso")
    
    def create_predictions_table(self, table_name: str = 'weather_predictions') -> None:
        """
        Cria tabela para predições do modelo.
        
        Args:
            table_name: Nome da tabela
        """
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            estacao VARCHAR(50) NOT NULL,
            temperatura_real FLOAT,
            temperatura_prevista FLOAT,
            erro_predicao FLOAT,
            erro_absoluto FLOAT,
            rmse_test FLOAT,
            data_processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(timestamp, estacao)
        );
        
        CREATE INDEX IF NOT EXISTS idx_pred_timestamp ON {table_name}(timestamp);
        CREATE INDEX IF NOT EXISTS idx_pred_estacao ON {table_name}(estacao);
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        
        print(f"✅ Tabela '{table_name}' criada com sucesso")
    
    def close(self) -> None:
        """Fecha conexão."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None


# Funções auxiliares para uso direto em notebooks

def get_neon_engine(env_path: Optional[str] = None) -> Engine:
    """
    Cria e retorna engine SQLAlchemy para Neon.
    
    Args:
        env_path: Caminho para arquivo .env
        
    Returns:
        Engine SQLAlchemy
        
    Example:
        >>> engine = get_neon_engine()
        >>> df = pd.read_sql("SELECT * FROM tabela", engine)
    """
    conn = NeonConnection(env_path)
    return conn.engine


def quick_query(query: str, env_path: Optional[str] = None) -> pd.DataFrame:
    """
    Executa query rápida e retorna DataFrame.
    
    Args:
        query: Query SQL
        env_path: Caminho para arquivo .env
        
    Returns:
        DataFrame com resultados
        
    Example:
        >>> df = quick_query("SELECT * FROM weather_data LIMIT 10")
    """
    conn = NeonConnection(env_path)
    return conn.execute_query(query)


def save_to_neon(
    df: pd.DataFrame,
    table_name: str,
    if_exists: str = 'append',
    env_path: Optional[str] = None
) -> int:
    """
    Salva DataFrame no Neon.
    
    Args:
        df: DataFrame para salvar
        table_name: Nome da tabela
        if_exists: 'fail', 'replace', ou 'append'
        env_path: Caminho para arquivo .env
        
    Returns:
        Número de linhas salvas
        
    Example:
        >>> rows = save_to_neon(df, 'weather_data', if_exists='append')
        >>> print(f"Salvo {rows} linhas")
    """
    conn = NeonConnection(env_path)
    return conn.save_dataframe(df, table_name, if_exists=if_exists)


if __name__ == '__main__':
    # Teste básico
    print("🧪 Testando conexão com Neon...\n")
    
    try:
        conn = NeonConnection()
        
        if conn.test_connection():
            print("✅ Conexão estabelecida!")
            
            tables = conn.get_tables()
            print(f"\n📋 Tabelas disponíveis: {len(tables)}")
            for table in tables:
                print(f"   • {table}")
        else:
            print("❌ Falha na conexão")
            
    except Exception as e:
        print(f"❌ Erro: {e}")
