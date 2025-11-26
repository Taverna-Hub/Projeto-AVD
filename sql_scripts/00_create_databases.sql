-- Create additional databases required by services
-- This script runs first (00_) to ensure databases exist before other init scripts

-- Create MLflow database
CREATE DATABASE mlflow;

-- Create Trendz database
CREATE DATABASE trendz;

-- Create NEON database
CREATE DATABASE meteorologia;

-- Note: 'thingsboard' database is created by default via POSTGRES_DB env variable
