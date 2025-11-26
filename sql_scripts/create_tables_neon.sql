CREATE TABLE dados_meteorologicos (
    id SERIAL PRIMARY KEY,
    estacao VARCHAR(50),
    data DATE,
    temperatura FLOAT,
    umidade FLOAT,
    velocidade_vento FLOAT,
    sensacao_termica FLOAT
);