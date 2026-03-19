# Desafio Engenheiro de Dados - ENEM

Este projeto é uma solução para o teste técnico de Engenharia de Dados baseado nos microdados do ENEM 2020. A solução foi arquitetada pensando em performance, isolamento e melhores práticas usando **PySpark**, **Docker**, **MySQL** e Modelagem Dimensional (**Star Schema**).

## 🚀 Como Rodar o Projeto

Para garantir o melhor desempenho, o projeto é totalmente contêinerizado. Você só precisa ter o Docker e Docker Compose instalados. Todas as dependências (Java, Spark, Python, MySQL) são gerenciadas automaticamente!

1. **Abra o terminal** na pasta raiz do projeto (`C:\Users\Dell\Documents\codigo\enem` ou no seu WSL).
2. **Suba o banco de dados MySQL** em background:
   ```bash
   docker-compose up -d db
   ```
   *Nota: o MySQL já vai subir criando automaticamente as tabelas dimensionais e de fatos através do `init.sql` lido automaticamente no entrypoint.*
   
3. **Aguarde alguns segundos** (cerca de 10-15s) para o banco de dados inicializar por completo e aceitar conexões.

4. **Execute o Job de ETL em PySpark**:
   ```bash
   docker-compose build etl
   docker-compose up etl
   ```
   *O PySpark lerá os 2GB do CSV na sua pasta `dados/MICRODADOS_ENEM_2020.csv`, fará o tratamento relacional dos dados e irá usar JDBC Connector para gravar os DataFrames de Dimensão e Fato no MySQL com ótima escalabilidade. Você poderá acompanhar o log do Spark diretamente no terminal.*

## 📊 Estrutura e Modelagem de Dados

A base foi modelada utilizando o **Esquema Estrela (Star Schema)** contendo:
- `dim_aluno` (Perfil do inscrito)
- `dim_escola` (Agrupamento dos dados das escolas)
- `dim_localidade_prova` (Locais de realização)
- `fato_resultados` (Fato com chaves estrangeiras, presenças e as notas em si)

## 📌 Indicadores e Consultas Analíticas

Ao fim do processo, abra sua IDE preferida de Banco de Dados localmente (DBeaver, MySQL Workbench, etc.) apontando para `localhost:3306` com usuário `root` e senha `root`. 

O arquivo **`queries.sql`** na raiz do projeto já contém as instruções SQL exatas desenvolvidas para resolver as 8 perguntas levantadas pelo desafio:
1. Qual a escola com a maior média de notas?
2. Qual o aluno com a maior média de notas?
3. Qual a média geral?
4. Qual o % de Ausentes?
5. Qual o número total de Inscritos?
6. Qual a média por disciplina?
7. Qual a média por Sexo?
8. Qual a média por Etnia?

## 🛠 Tecnologias Utilizadas
- **Docker & Docker Compose**
- **Python 3.10+ e PySpark 3.4.1**
- **MySQL 8.0**
- **Paradigma:** Data Engineering, BDD ETL, Dimensional Modeling.
