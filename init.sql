CREATE DATABASE IF NOT EXISTS enem_db;
USE enem_db;

-- Dimensão Aluno
CREATE TABLE IF NOT EXISTS dim_aluno (
    nu_inscricao BIGINT PRIMARY KEY,
    tp_sexo VARCHAR(2),
    tp_faixa_etaria INT,
    tp_cor_raca INT
);

-- Dimensão Escola (Usando Municipio da Escola como agregador primário devido à ausência do CO_ESCOLA em 2020)
CREATE TABLE IF NOT EXISTS dim_escola (
    co_municipio_esc INT PRIMARY KEY,
    tp_escola INT,
    no_municipio_esc VARCHAR(150),
    co_uf_esc INT,
    sg_uf_esc VARCHAR(2),
    tp_dependencia_adm_esc INT
);

-- Dimensão Localidade de Prova
CREATE TABLE IF NOT EXISTS dim_localidade_prova (
    co_municipio_prova INT PRIMARY KEY,
    no_municipio_prova VARCHAR(150),
    co_uf_prova INT,
    sg_uf_prova VARCHAR(2)
);

-- Tabela de Fato Fato_Resultados
CREATE TABLE IF NOT EXISTS fato_resultados (
    id_fato BIGINT AUTO_INCREMENT PRIMARY KEY,
    nu_inscricao BIGINT,
    co_municipio_esc INT,
    co_municipio_prova INT,
    tp_presenca_cn INT,
    tp_presenca_ch INT,
    tp_presenca_lc INT,
    tp_presenca_mt INT,
    nota_cn FLOAT,
    nota_ch FLOAT,
    nota_lc FLOAT,
    nota_mt FLOAT,
    nota_redacao FLOAT,
    media_notas FLOAT,
    FOREIGN KEY (nu_inscricao) REFERENCES dim_aluno(nu_inscricao),
    FOREIGN KEY (co_municipio_esc) REFERENCES dim_escola(co_municipio_esc),
    FOREIGN KEY (co_municipio_prova) REFERENCES dim_localidade_prova(co_municipio_prova)
);
