import boto3
import psycopg2
from psycopg2.extras import execute_values
import csv
from io import StringIO
from dotenv import load_dotenv
import os
import sys
from datetime import datetime
import hashlib

# Carregar variáveis de ambiente do arquivo .env
load_dotenv(override=True)

# Configurações do AWS S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

# Função para obter conexão com o banco
def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("NEON_DB"),
        user=os.getenv("NEON_USER"),
        password=os.getenv("NEON_PASSWORD"),
        host=os.getenv("NEON_HOST"),
        port=os.getenv("NEON_PORT")
    )

# Função para inicializar o banco de dados
def initialize_database():
    """Cria as tabelas se não existirem"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Verificar se a tabela dados_meteorologicos existe
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'dados_meteorologicos'
            );
        """)
        tabela_existe = cursor.fetchone()[0]
        
        if not tabela_existe:
            print(" Criando tabela dados_meteorologicos...")
            cursor.execute("""
                CREATE TABLE dados_meteorologicos (
                    id SERIAL PRIMARY KEY,
                    estacao VARCHAR(50),
                    data DATE,
                    hora VARCHAR(10),
                    temperatura FLOAT,
                    umidade FLOAT,
                    velocidade_vento FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(estacao, data, hora)
                );
            """)
            print("✓ Tabela dados_meteorologicos criada!")
        else:
            print("✓ Tabela dados_meteorologicos já existe")
            
            # Verificar e adicionar coluna hora se não existir
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_name = 'dados_meteorologicos' 
                    AND column_name = 'hora'
                );
            """)
            coluna_hora_existe = cursor.fetchone()[0]
            
            if not coluna_hora_existe:
                print(" Adicionando coluna hora...")
                cursor.execute("""
                    ALTER TABLE dados_meteorologicos ADD COLUMN hora VARCHAR(10);
                """)
                print("✓ Coluna hora adicionada!")
            
            # Verificar e adicionar coluna updated_at se não existir
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_name = 'dados_meteorologicos' 
                    AND column_name = 'updated_at'
                );
            """)
            coluna_updated_existe = cursor.fetchone()[0]
            
            if not coluna_updated_existe:
                print(" Adicionando coluna updated_at...")
                cursor.execute("""
                    ALTER TABLE dados_meteorologicos ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                """)
                print("✓ Coluna updated_at adicionada!")
            
            # Verificar e adicionar constraint UNIQUE se não existir
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.table_constraints 
                    WHERE table_name = 'dados_meteorologicos' 
                    AND constraint_name = 'dados_meteorologicos_estacao_data_hora_key'
                );
            """)
            constraint_existe = cursor.fetchone()[0]
            
            if not constraint_existe:
                print(" Adicionando constraint UNIQUE(estacao, data, hora)...")
                cursor.execute("""
                    ALTER TABLE dados_meteorologicos 
                    ADD CONSTRAINT dados_meteorologicos_estacao_data_hora_key 
                    UNIQUE(estacao, data, hora);
                """)
                print("✓ Constraint UNIQUE adicionada!")
        
        # Verificar se a tabela arquivos_processados existe
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'arquivos_processados'
            );
        """)
        tabela_arquivos_existe = cursor.fetchone()[0]
        
        if not tabela_arquivos_existe:
            print(" Criando tabela arquivos_processados...")
            cursor.execute("""
                CREATE TABLE arquivos_processados (
                    id SERIAL PRIMARY KEY,
                    arquivo_s3 VARCHAR(500) UNIQUE,
                    data_processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    registros_inseridos INTEGER DEFAULT 0,
                    registros_atualizados INTEGER DEFAULT 0,
                    status VARCHAR(20)
                );
            """)
            print("✓ Tabela arquivos_processados criada!")
        else:
            print("✓ Tabela arquivos_processados já existe")
        
        # Criar índices
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_estacao_data 
            ON dados_meteorologicos(estacao, data);
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_arquivo 
            ON arquivos_processados(arquivo_s3);
        """)
        
        conn.commit()
        print("✓ Banco de dados inicializado com sucesso!")
    except Exception as e:
        print(f"✗ Erro ao inicializar banco de dados: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

# Função para verificar se arquivo já foi processado
def arquivo_ja_processado(arquivo_s3):
    """Verifica se o arquivo já foi processado com sucesso"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT status FROM arquivos_processados 
            WHERE arquivo_s3 = %s AND status = 'sucesso'
        """, (arquivo_s3,))
        resultado = cursor.fetchone()
        return resultado is not None
    except Exception as e:
        print(f"✗ Erro ao verificar arquivo: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

# Função para registrar arquivo processado
def registrar_arquivo_processado(arquivo_s3, registros_inseridos, registros_atualizados, status):
    """Registra ou atualiza o processamento de um arquivo"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO arquivos_processados 
                (arquivo_s3, registros_inseridos, registros_atualizados, status)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (arquivo_s3) 
            DO UPDATE SET 
                data_processamento = CURRENT_TIMESTAMP,
                registros_inseridos = EXCLUDED.registros_inseridos,
                registros_atualizados = EXCLUDED.registros_atualizados,
                status = EXCLUDED.status;
        """, (arquivo_s3, registros_inseridos, registros_atualizados, status))
        conn.commit()
    except Exception as e:
        print(f"✗ Erro ao registrar arquivo: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Função para listar objetos no S3
def list_s3_objects(bucket_name, prefix):
    print(f"Listando objetos no bucket {bucket_name} com prefixo {prefix}...")
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if 'Contents' in response:
        return [obj['Key'] for obj in response['Contents']]
    else:
        print("Nenhum objeto encontrado no prefixo especificado.")
        return []

# Função para extrair dados de um objeto no S3
def extract_from_s3(bucket_name, object_name):
    print(f"Extraindo dados do objeto {object_name} no bucket {bucket_name}...")
    response = s3_client.get_object(Bucket=bucket_name, Key=object_name)
    content = response['Body'].read()
    try:
        return content.decode('utf-8')
    except UnicodeDecodeError:
        print("Falha ao decodificar UTF-8, tentando Latin-1...")
        return content.decode('latin-1')

# Função para carregar dados no NEON
def load_to_neon(data, arquivo_s3):
    print("Carregando dados no NEON (Modo Batch)...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    lines = data.splitlines()
    estacao = None
    header_found = False
    
    # Buffer para batch insert
    batch_rows = []
    BATCH_SIZE = 1000  # Envia 1000 linhas por vez
    
    total_processed = 0
    
    try:
        for line in lines:
            # Extract station name from metadata
            if line.startswith("ESTACAO:"):
                parts = line.split(";")
                if len(parts) > 1:
                    estacao = parts[1].strip()
                continue
                
            # Skip metadata until header
            if not header_found:
                if line.upper().startswith("DATA;HORA UTC"):
                    header_found = True
                continue
                
            # Process data rows
            row = line.split(";")
            if len(row) < 19: # Ensure enough columns
                continue
                
            # Map columns
            # Data: index 0
            # Hora: index 1
            # Temperatura: index 7
            # Umidade: index 15
            # Velocidade Vento: index 18
            
            data_medicao = row[0]
            hora_medicao = row[1]  
            
            # Convert date format
            try:
                # Clean date format YYYY/MM/DD to YYYY-MM-DD
                if "/" in data_medicao:
                    data_medicao = data_medicao.replace("/", "-")
                
                # Parse date
                data_obj = datetime.strptime(data_medicao, "%Y-%m-%d").date()
            except (ValueError, IndexError) as e:
                # print(f"⚠ Erro ao processar data: {data_medicao} - {e}")
                continue
            
            def parse_float(value):
                if not value:
                    return None
                try:
                    return float(value.replace(",", "."))
                except ValueError:
                    return None

            temperatura = parse_float(row[7])
            umidade = parse_float(row[15])
            velocidade_vento = parse_float(row[18])
            
            # Adicionar à lista de batch (tupla com os dados)
            batch_rows.append((
                estacao, 
                data_obj, 
                hora_medicao, 
                temperatura, 
                umidade, 
                velocidade_vento, 
                datetime.now()
            ))
            
            # Se o buffer encheu, executa o batch
            if len(batch_rows) >= BATCH_SIZE:
                execute_values(cursor, """
                    INSERT INTO dados_meteorologicos 
                        (estacao, data, hora, temperatura, umidade, velocidade_vento, updated_at) 
                    VALUES %s
                    ON CONFLICT (estacao, data, hora) 
                    DO UPDATE SET 
                        temperatura = EXCLUDED.temperatura,
                        umidade = EXCLUDED.umidade,
                        velocidade_vento = EXCLUDED.velocidade_vento,
                        updated_at = EXCLUDED.updated_at
                """, batch_rows)
                conn.commit()
                total_processed += len(batch_rows)
                print(f"  ✓ Processados: {total_processed} linhas...")
                batch_rows = [] # Limpa o buffer

        # Processar o restante das linhas que sobraram no buffer
        if batch_rows:
            execute_values(cursor, """
                INSERT INTO dados_meteorologicos 
                    (estacao, data, hora, temperatura, umidade, velocidade_vento, updated_at) 
                VALUES %s
                ON CONFLICT (estacao, data, hora) 
                DO UPDATE SET 
                    temperatura = EXCLUDED.temperatura,
                    umidade = EXCLUDED.umidade,
                    velocidade_vento = EXCLUDED.velocidade_vento,
                    updated_at = EXCLUDED.updated_at
            """, batch_rows)
            conn.commit()
            total_processed += len(batch_rows)

    except Exception as e:
        print(f"  ✗ Erro ao processar batch: {e}")
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()
    
    # Registrar arquivo como processado
    registrar_arquivo_processado(arquivo_s3, total_processed, 0, 'sucesso')
    
    print(f"✓ Arquivo processado! Total de linhas: {total_processed}")

# Pipeline principal
def main():
    bucket_name = os.getenv("AWS_BUCKET_NAME")
    prefix = os.getenv("S3_PREFIX")

    if not bucket_name or not prefix:
        print("Erro: Variáveis de ambiente AWS_BUCKET_NAME ou S3_PREFIX não configuradas.")
        sys.exit(1)

    # Inicializar banco de dados (criar tabelas)
    print("=== Inicializando banco de dados ===")
    initialize_database()
    
    print(f"\n=== Listando objetos no bucket {bucket_name} com prefixo {prefix} ===")
    objects = list_s3_objects(bucket_name, prefix)
    
    total_arquivos = len(objects)
    processados = 0
    ignorados = 0
    
    # Processar cada objeto
    for idx, object_name in enumerate(objects, 1):
        print(f"\n[{idx}/{total_arquivos}] Verificando arquivo: {object_name}")
        
        # Verificar se arquivo já foi processado
        if arquivo_ja_processado(object_name):
            print(f"  ⏭ Arquivo já processado anteriormente. Pulando...")
            ignorados += 1
            continue
        
        try:
            print(f"  📥 Extraindo dados do S3...")
            data = extract_from_s3(bucket_name, object_name)
            
            print(f"  💾 Carregando no Neon...")
            load_to_neon(data, object_name)
            
            processados += 1
        except Exception as e:
            print(f"  ✗ Erro ao processar arquivo: {e}")
            registrar_arquivo_processado(object_name, 0, 0, 'erro')
            continue
    
    print(f"\n{'='*60}")
    print(f"Pipeline concluído!")
    print(f"  Total de arquivos encontrados: {total_arquivos}")
    print(f"  Arquivos processados: {processados}")
    print(f"  Arquivos ignorados (já processados): {ignorados}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()