import os
import sys
import unicodedata
from pathlib import Path


def normalize_text(text):
    text = unicodedata.normalize('NFD', text)
    
    text = ''.join(char for char in text if unicodedata.category(char) != 'Mn')
    
    text = text.upper()
    
    replacements = {
        'Ç': 'C',
    }
    
    for old, new in replacements.items():
        text = text.replace(old, new)
    
    return text


def fix_csv_encoding(file_path, backup=True):
    try:
        with open(file_path, 'r', encoding='latin-1') as f:
            lines = f.readlines()
        
        if backup:
            backup_path = str(file_path) + '.bak'
            with open(backup_path, 'w', encoding='latin-1') as f:
                f.writelines(lines)
            print(f"  Backup criado: {backup_path}")
        
        if len(lines) > 8:
            header_line = lines[8]
            normalized_header = normalize_text(header_line)
            lines[8] = normalized_header
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        
        print(f"✓ Corrigido: {file_path}")
        return True
    
    except Exception as e:
        print(f"✗ Erro ao processar {file_path}: {str(e)}")
        return False


def process_data_directory(data_dir, backup=True):
    data_path = Path(data_dir)
    
    if not data_path.exists():
        print(f"Erro: Diretório não encontrado: {data_dir}")
        sys.exit(1)
    
    csv_files = list(data_path.rglob("*.CSV")) + list(data_path.rglob("*.csv"))
    
    if not csv_files:
        print("Nenhum arquivo CSV encontrado.")
        return
    
    print(f"Encontrados {len(csv_files)} arquivos CSV para processar.\n")
    
    success_count = 0
    error_count = 0
    
    for csv_file in sorted(csv_files):
        if fix_csv_encoding(csv_file, backup=backup):
            success_count += 1
        else:
            error_count += 1
    
    print(f"\n{'='*60}")
    print(f"Resumo:")
    print(f"  Arquivos processados com sucesso: {success_count}")
    print(f"  Arquivos com erro: {error_count}")
    print(f"  Total: {len(csv_files)}")
    
    if backup:
        print(f"\nOs arquivos originais foram salvos com extensão .bak")
        print(f"Para remover os backups, execute:")
        print(f"  find {data_dir} -name '*.bak' -delete")


def main():
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent
    data_dir = project_root / "data"
    
    print("="*60)
    print("Script de Correção de Codificação de Arquivos CSV")
    print("="*60)
    print(f"Diretório de dados: {data_dir}\n")
    
    response = input("Deseja criar backups dos arquivos originais? (S/n): ").strip().lower()
    backup = response != 'n'
    
    print()
    process_data_directory(data_dir, backup=backup)


if __name__ == "__main__":
    main()
