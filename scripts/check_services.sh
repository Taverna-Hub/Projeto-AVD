#!/bin/bash

# Script para verificar o status de todos os serviços

echo "========================================="
echo "Verificando status dos serviços..."
echo "========================================="
echo ""

# Cores para output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Função para verificar serviço
check_service() {
    local name=$1
    local url=$2
    
    echo -n "Verificando $name... "
    
    if curl -s -o /dev/null -w "%{http_code}" "$url" > /dev/null 2>&1; then
        status=$(curl -s -o /dev/null -w "%{http_code}" "$url")
        if [ "$status" -eq 200 ] || [ "$status" -eq 302 ] || [ "$status" -eq 401 ]; then
            echo -e "${GREEN}✓ OK${NC} (HTTP $status)"
            return 0
        else
            echo -e "${YELLOW}⚠ Resposta${NC} (HTTP $status)"
            return 1
        fi
    else
        echo -e "${RED}✗ Não acessível${NC}"
        return 1
    fi
}

# Verificar cada serviço
check_service "FastAPI" "http://localhost:8080"
check_service "FastAPI Health" "http://localhost:8080/health"
check_service "ThingsBoard" "http://localhost:9090"
check_service "JupyterLab" "http://localhost:8888"
check_service "Trendz Analytics" "http://localhost:8889"
check_service "MLflow" "http://localhost:5000"

echo ""
echo "========================================="
echo "Status dos Containers Docker"
echo "========================================="
docker-compose ps

echo ""
echo "========================================="
echo "URLs dos Serviços"
echo "========================================="
echo ""
echo "FastAPI:"
echo "  - API: http://localhost:8080"
echo "  - Docs: http://localhost:8080/docs"
echo "  - Redoc: http://localhost:8080/redoc"
echo ""
echo "ThingsBoard:"
echo "  - Web UI: http://localhost:9090"
echo "  - Login: tenant@thingsboard.org / tenant"
echo ""
echo "JupyterLab:"
echo "  - Web UI: http://localhost:8888"
echo "  - Sem senha"
echo ""
echo "Trendz Analytics:"
echo "  - Web UI: http://localhost:8889"
echo ""
echo "MLflow:"
echo "  - Web UI: http://localhost:5000"
echo ""
echo "PostgreSQL:"
echo "  - Host: localhost:5432"
echo "  - User: postgres"
echo "  - Pass: postgres"
echo ""
