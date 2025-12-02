import os
import psycopg2
import boto3
import pandas as pd
from datetime import datetime
import time
import sys

# Configurações do Neon
NEON_CONFIG = {
    'host': os.getenv('NEON_HOST'),
    'port': os.getenv('NEON_PORT', '5432'),
    'database': os.getenv('NEON_DB'),
    'user': os.getenv('NEON_USER'),
    'password': os.getenv('NEON_PASSWORD')
}

# Configurações S3
S3_CONFIG = {
    'bucket': os.getenv('AWS_BUCKET_NAME'),
    'prefix': os.getenv('S3_PREFIX', 'neon-exports'),
    'region': os.getenv('AWS_REGION', 'us-east-1')
}

def wait_for_database():
    """Aguarda o banco estar disponível"""
    max_retries = 30
    retry_count = 0
    
    print("=== Verificando conexão com Neon ===")
    while retry_count < max_retries:
        try:
            conn = psycopg2.connect(**NEON_CONFIG)
            conn.close()
            print("✓ Conexão com Neon estabelecida!")
            return True
        except psycopg2.OperationalError as e:
            retry_count += 1
            print(f" Aguardando Neon... tentativa {retry_count}/{max_retries}")
            print(f"   Erro: {str(e)[:100]}")
            time.sleep(2)
        except Exception as e:
            print(f"✗ Erro inesperado ao conectar ao Neon: {e}")
            sys.exit(1)
    
    print("✗ Não foi possível conectar ao Neon após múltiplas tentativas")
    sys.exit(1)

def initialize_database():
    """Cria as tabelas iniciais se não existirem"""
    try:
        conn = psycopg2.connect(**NEON_CONFIG)
        cursor = conn.cursor()
        
        print("=== Inicializando banco de dados ===")
        
        # Criar tabela se não existir
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dados_meteorologicos (
                id SERIAL PRIMARY KEY,
                estacao VARCHAR(50),
                data DATE,
                temperatura FLOAT,
                umidade FLOAT,
                velocidade_vento FLOAT,
                sensacao_termica FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Criar índice para melhorar performance
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_estacao_data 
            ON dados_meteorologicos(estacao, data);
        """)
        
        conn.commit()
        
        # Verificar quantidade de registros
        cursor.execute("SELECT COUNT(*) FROM dados_meteorologicos")
        count = cursor.fetchone()[0]
        print(f"✓ Tabelas verificadas/criadas! Total de registros: {count}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"✗ Erro ao inicializar banco de dados: {e}")
        sys.exit(1)

def export_to_s3():
    """Exporta dados do Neon para S3 em formato Parquet"""
    try:
        print("\n=== Exportando dados para S3 ===")
        
        conn = psycopg2.connect(**NEON_CONFIG)
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=S3_CONFIG['region']
        )
        
        # Verificar se há dados
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM dados_meteorologicos")
        count = cursor.fetchone()[0]
        
        if count == 0:
            print("⚠ Nenhum dado para exportar")
            cursor.close()
            conn.close()
            return
        
        print(f" Exportando {count} registros...")
        
        # Exportar dados
        query = """
            SELECT 
                id, estacao, data, temperatura, umidade, 
                velocidade_vento, sensacao_termica, created_at
            FROM dados_meteorologicos
            ORDER BY data DESC, estacao
        """
        df = pd.read_sql(query, conn)
        
        # Salvar no S3
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        key = f"{S3_CONFIG['prefix']}/exports/meteorologia_{timestamp}.parquet"
        
        temp_file = f'/tmp/temp_{timestamp}.parquet'
        df.to_parquet(temp_file, index=False)
        
        s3_client.upload_file(temp_file, S3_CONFIG['bucket'], key)
        
        # Remover arquivo temporário
        os.remove(temp_file)
        
        print(f"✓ {len(df)} registros exportados para s3://{S3_CONFIG['bucket']}/{key}")
        print(f"   Tamanho do arquivo: {os.path.getsize(temp_file) / 1024:.2f} KB")
        
        cursor.close()
        conn.close()
        
    except FileNotFoundError:
        print("⚠ Arquivo temporário não encontrado (já foi removido)")
    except Exception as e:
        print(f"✗ Erro na exportação: {e}")

def get_database_stats():
    """Exibe estatísticas do banco de dados"""
    try:
        conn = psycopg2.connect(**NEON_CONFIG)
        cursor = conn.cursor()
        
        print("\n=== Estatísticas do Banco ===")
        
        # Total de registros
        cursor.execute("SELECT COUNT(*) FROM dados_meteorologicos")
        total = cursor.fetchone()[0]
        print(f" Total de registros: {total}")
        
        # Registros por estação
        cursor.execute("""
            SELECT estacao, COUNT(*) as total 
            FROM dados_meteorologicos 
            GROUP BY estacao 
            ORDER BY total DESC 
            LIMIT 5
        """)
        print("\n Top 5 estações com mais dados:")
        for estacao, count in cursor.fetchall():
            print(f"   • {estacao}: {count} registros")
        
        # Período dos dados
        cursor.execute("""
            SELECT MIN(data) as primeira_data, MAX(data) as ultima_data 
            FROM dados_meteorologicos
        """)
        result = cursor.fetchone()
        if result[0]:
            print(f"\n Período: {result[0]} até {result[1]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"✗ Erro ao obter estatísticas: {e}")

if __name__ == '__main__':
    print("="*50)
    print("  Pipeline Neon → S3 - Dados Meteorológicos")
    print("="*50)
    
    # Verificar variáveis de ambiente
    required_vars = ['NEON_HOST', 'NEON_USER', 'NEON_PASSWORD', 'NEON_DB']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"✗ Variáveis de ambiente faltando: {', '.join(missing_vars)}")
        sys.exit(1)
    
    # Executar pipeline
    wait_for_database()
    initialize_database()
    get_database_stats()
    
    # Loop de exportação periódica
    export_interval = int(os.getenv('EXPORT_INTERVAL_SECONDS', 3600))
    print(f"\n Pipeline ativo. Exportando a cada {export_interval/60:.0f} minutos...")
    
    try:
        while True:
            export_to_s3()
            print(f"\n⏰ Próxima exportação em {export_interval/60:.0f} minutos...")
            time.sleep(export_interval)
    except KeyboardInterrupt:
        print("\n\n Pipeline encerrado pelo usuário")
    except Exception as e:
        print(f"\n✗ Erro fatal no pipeline: {e}")
        sys.exit(1)
