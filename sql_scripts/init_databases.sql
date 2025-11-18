-- Script de inicialização do banco de dados
-- Executar após o primeiro start do docker-compose

-- Criar databases adicionais se necessário
-- (mlflow e trendz já devem existir)

-- Conectar ao banco thingsboard e criar extensões úteis
\c thingsboard;

-- Habilitar extensões úteis
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Criar schema para dados customizados
CREATE SCHEMA IF NOT EXISTS custom_data;

-- Tabela de exemplo para dados de sensores
CREATE TABLE IF NOT EXISTS custom_data.sensor_readings (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(100) NOT NULL,
    sensor_type VARCHAR(50) NOT NULL,
    value NUMERIC(10, 2) NOT NULL,
    unit VARCHAR(20),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_sensor_readings_device_id ON custom_data.sensor_readings(device_id);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_timestamp ON custom_data.sensor_readings(timestamp);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_sensor_type ON custom_data.sensor_readings(sensor_type);

-- Tabela para logs de API
CREATE TABLE IF NOT EXISTS custom_data.api_logs (
    id SERIAL PRIMARY KEY,
    endpoint VARCHAR(255) NOT NULL,
    method VARCHAR(10) NOT NULL,
    status_code INTEGER,
    response_time_ms INTEGER,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    user_agent TEXT,
    ip_address INET
);

CREATE INDEX IF NOT EXISTS idx_api_logs_timestamp ON custom_data.api_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_api_logs_endpoint ON custom_data.api_logs(endpoint);

-- Conectar ao banco mlflow
\c mlflow;

-- MLflow já cria suas próprias tabelas automaticamente
-- Apenas garantir que o schema public existe
CREATE SCHEMA IF NOT EXISTS public;

-- Conectar ao banco trendz
\c trendz;

-- Trendz também gerencia suas próprias tabelas
CREATE SCHEMA IF NOT EXISTS public;

-- Voltar ao banco postgres
\c postgres;

-- Listar todos os bancos
\l

-- Mensagem de sucesso
SELECT 'Databases inicializados com sucesso!' AS status;
