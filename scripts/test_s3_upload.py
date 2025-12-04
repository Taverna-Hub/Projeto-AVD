"""
Script de teste para validar upload de arquivos CSV para S3.
Testa o serviço S3 com upload de uma estação.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Adicionar o diretório raiz ao path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from fastapi.services.s3_service import create_s3_service
from fastapi.services.device_manager_service import ESTACOES_METEOROLOGICAS

# Carregar variáveis de ambiente
env_path = project_root / "fastapi" / ".env"
if env_path.exists():
    load_dotenv(env_path)
else:
    print(f"⚠️  Arquivo .env não encontrado em {env_path}")
    print("Configure as variáveis de ambiente S3:")
    print("- S3_BUCKET_NAME")
    print("- AWS_ACCESS_KEY_ID")
    print("- AWS_SECRET_ACCESS_KEY")
    print("- AWS_REGION (opcional)")
    sys.exit(1)


def test_s3_connection():
    """Testa conexão com S3."""
    print("\n" + "="*60)
    print("TESTE 1: Verificando conexão com S3")
    print("="*60)
    
    try:
        s3_service = create_s3_service(
            bucket_name=os.getenv("S3_BUCKET_NAME"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            aws_region=os.getenv("AWS_REGION", "us-east-1"),
            s3_prefix="data"
        )
        
        print(f"✓ Cliente S3 criado com sucesso")
        print(f"  Bucket: {s3_service.bucket_name}")
        print(f"  Região: {os.getenv('AWS_REGION', 'us-east-1')}")
        print(f"  Prefixo: {s3_service.s3_prefix}")
        
        # Verificar se bucket existe
        if s3_service.check_bucket_exists():
            print(f"✓ Bucket '{s3_service.bucket_name}' está acessível")
            return s3_service
        else:
            print(f"✗ Bucket '{s3_service.bucket_name}' não está acessível")
            return None
            
    except Exception as e:
        print(f"✗ Erro ao criar cliente S3: {str(e)}")
        return None


def test_list_files(s3_service):
    """Lista arquivos no bucket."""
    print("\n" + "="*60)
    print("TESTE 2: Listando arquivos no bucket")
    print("="*60)
    
    try:
        arquivos = s3_service.listar_arquivos_bucket()
        print(f"✓ Encontrados {len(arquivos)} arquivos no bucket")
        
        if arquivos:
            print("\nPrimeiros 5 arquivos:")
            for arquivo in arquivos[:5]:
                print(f"  - {arquivo}")
        
        return True
        
    except Exception as e:
        print(f"✗ Erro ao listar arquivos: {str(e)}")
        return False


def test_upload_single_station(s3_service):
    """Testa upload de uma estação."""
    print("\n" + "="*60)
    print("TESTE 3: Upload de arquivos de uma estação (ARCO_VERDE)")
    print("="*60)
    
    try:
        # Diretório de dados
        data_directory = project_root / "data"
        
        if not data_directory.exists():
            print(f"✗ Diretório de dados não encontrado: {data_directory}")
            return False
        
        print(f"✓ Diretório de dados: {data_directory}")
        
        # Testar com ARCO_VERDE apenas em 2020
        resultado = s3_service.upload_todos_csv_estacao(
            data_directory=data_directory,
            estacao_nome="ARCO_VERDE",
            anos=["2020"]
        )
        
        print(f"\nResultado do upload:")
        print(f"  Estação: {resultado['estacao']}")
        print(f"  Total de arquivos: {resultado['total']}")
        print(f"  Sucesso: {resultado['sucesso']}")
        print(f"  Falhas: {resultado['falhas']}")
        
        if resultado['sucesso'] > 0:
            print(f"\n✓ Upload bem-sucedido!")
            print(f"\nDetalhes dos arquivos:")
            for arquivo_info in resultado['arquivos']:
                status_icon = "✓" if arquivo_info['status'] == 'sucesso' else "✗"
                print(f"  {status_icon} {arquivo_info['arquivo']}")
                if arquivo_info['status'] == 'sucesso':
                    print(f"    S3: {arquivo_info['s3_key']}")
            return True
        else:
            print(f"✗ Nenhum arquivo foi enviado com sucesso")
            return False
            
    except Exception as e:
        print(f"✗ Erro no upload: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_upload_all_stations(s3_service):
    """Testa upload de todas as estações (anos 2020-2024)."""
    print("\n" + "="*60)
    print("TESTE 4: Upload de todas as estações (anos 2020-2024)")
    print("="*60)
    
    try:
        # Diretório de dados
        data_directory = project_root / "data"
        
        # Upload de todos os anos disponíveis (2020-2024)
        resultado = s3_service.upload_todas_estacoes(
            data_directory=data_directory,
            estacoes=ESTACOES_METEOROLOGICAS,
            anos=None  # None = todos os anos disponíveis
        )
        
        print(f"\nResultado geral:")
        print(f"  Total de estações: {resultado['total_estacoes']}")
        print(f"  Total de arquivos: {resultado['total_arquivos']}")
        print(f"  Sucesso: {resultado['total_sucesso']}")
        print(f"  Falhas: {resultado['total_falhas']}")
        
        if resultado['total_sucesso'] > 0:
            taxa = (resultado['total_sucesso'] / resultado['total_arquivos'] * 100)
            print(f"  Taxa de sucesso: {taxa:.2f}%")
            
            print(f"\n✓ Upload concluído!")
            print(f"\nEstações processadas:")
            for estacao in resultado['estacoes_processadas']:
                detalhe = resultado['detalhes'][estacao]
                print(f"  - {estacao}: {detalhe['sucesso']}/{detalhe['total']} arquivos")
            
            return True
        else:
            print(f"✗ Nenhum arquivo foi enviado com sucesso")
            return False
            
    except Exception as e:
        print(f"✗ Erro no upload: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Função principal."""
    print("\n" + "="*60)
    print("TESTE DO SERVIÇO S3 - UPLOAD DE ARQUIVOS CSV")
    print("="*60)
    
    # Teste 1: Conectar ao S3
    s3_service = test_s3_connection()
    if not s3_service:
        print("\n❌ Falha na conexão com S3. Verifique as credenciais.")
        return
    
    # Teste 2: Listar arquivos
    test_list_files(s3_service)
    
    # Teste 3: Upload de uma estação
    if test_upload_single_station(s3_service):
        print("\n✓ Teste de upload de estação única passou!")
        
        # Teste 4: Upload de todas as estações (opcional)
        test_upload_all_stations(s3_service)
    
    print("\n" + "="*60)
    print("TESTES CONCLUÍDOS")
    print("="*60)


if __name__ == "__main__":
    main()
