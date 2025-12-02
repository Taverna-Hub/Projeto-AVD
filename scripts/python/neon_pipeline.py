import boto3
import psycopg2
import csv
from io import StringIO
from dotenv import load_dotenv
import os
import sys

# Carregar variáveis de ambiente do arquivo .env
load_dotenv(override=True)

# Configurações do AWS S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

# Configurações do NEON
conn = psycopg2.connect(
    dbname=os.getenv("NEON_DB"),
    user=os.getenv("NEON_USER"),
    password=os.getenv("NEON_PASSWORD"),
    host=os.getenv("NEON_HOST"),
    port=os.getenv("NEON_PORT")
)

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
def load_to_neon(data):
    print("Carregando dados no NEON...")
    cursor = conn.cursor()
    
    lines = data.splitlines()
    estacao = None
    header_found = False
    
    count_inserted = 0
    
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
        # Temperatura: index 7
        # Umidade: index 15
        # Velocidade Vento: index 18
        
        data_medicao = row[0]
        # Convert YYYY/MM/DD to YYYY-MM-DD
        if "/" in data_medicao:
            data_medicao = data_medicao.replace("/", "-")
        
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
        sensacao_termica = None # Not available in CSV
        
        cursor.execute(
            "INSERT INTO dados_meteorologicos (estacao, data, temperatura, umidade, velocidade_vento, sensacao_termica) VALUES (%s, %s, %s, %s, %s, %s)",
            (estacao, data_medicao, temperatura, umidade, velocidade_vento, sensacao_termica)
        )
        count_inserted += 1

    conn.commit()
    cursor.close()
    print(f"Dados carregados com sucesso no NEON! Total inserido: {count_inserted}")

# Pipeline principal
def main():
    bucket_name = os.getenv("AWS_BUCKET_NAME")
    prefix = os.getenv("S3_PREFIX")

    if not bucket_name or not prefix:
        print("Erro: Variáveis de ambiente AWS_BUCKET_NAME ou S3_PREFIX não configuradas.")
        sys.exit(1)

    print(f"Listando objetos no bucket {bucket_name} com prefixo {prefix}...")
    objects = list_s3_objects(bucket_name, prefix)
    
    # Processar cada objeto
    for object_name in objects:
        print(f"Processando objeto: {object_name}")
        data = extract_from_s3(bucket_name, object_name)
        load_to_neon(data)

if __name__ == "__main__":
    main()