#!/bin/bash

# Script para inicializar os bancos de dados necessários
# Execute este script após o primeiro start do docker-compose

echo "========================================="
echo "Inicializando bancos de dados..."
echo "========================================="
echo ""

# Aguardar PostgreSQL estar pronto
echo "Aguardando PostgreSQL iniciar..."
sleep 5

# Criar banco mlflow se não existir
echo "Criando banco de dados 'mlflow'..."
docker-compose exec -T postgres psql -U postgres -c "SELECT 'CREATE DATABASE mlflow' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mlflow')\gexec" 2>/dev/null || \
docker-compose exec -T postgres psql -U postgres -c "CREATE DATABASE mlflow;" 2>/dev/null || \
echo "  ✓ Banco 'mlflow' já existe"

# Criar banco trendz se não existir
echo "Criando banco de dados 'trendz'..."
docker-compose exec -T postgres psql -U postgres -c "SELECT 'CREATE DATABASE trendz' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'trendz')\gexec" 2>/dev/null || \
docker-compose exec -T postgres psql -U postgres -c "CREATE DATABASE trendz;" 2>/dev/null || \
echo "  ✓ Banco 'trendz' já existe"

echo ""
echo "========================================="
echo "Executando script de inicialização SQL..."
echo "========================================="

# Executar script de inicialização
docker-compose exec -T postgres psql -U postgres < ../sql_scripts/init_databases.sql

echo ""
echo "========================================="
echo "Listando bancos de dados criados..."
echo "========================================="

docker-compose exec -T postgres psql -U postgres -c "\l"

echo ""
echo "✓ Inicialização concluída!"
echo ""
echo "Aguarde alguns segundos para os serviços se conectarem aos bancos..."
