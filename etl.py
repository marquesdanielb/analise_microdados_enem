import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, when

# Configurações de Conexão com o BD (passadas pelo docker-compose)
MYSQL_HOST = os.environ.get('MYSQL_HOST', 'db')
MYSQL_PORT = os.environ.get('MYSQL_PORT', '3306')
MYSQL_USER = os.environ.get('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'root')
MYSQL_DB = os.environ.get('MYSQL_DB', 'enem_db')

JDBC_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
JDBC_PROPERTIES = {
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "driver": "com.mysql.cj.jdbc.Driver"
}

def main():
    print("Iniciando Spark Session...")
    spark = SparkSession.builder \
        .appName("ENEM_ETL") \
        .config("spark.jars", "/opt/spark/jars/mysql-connector-java-8.0.30.jar") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

    print("Carregando CSV do ENEM...")
    # Lendo o arquivo (Assumindo ISO-8859-1 que é comum no INEP)
    df = spark.read.csv(
        '/app/dados/MICRODADOS_ENEM_2020.csv', 
        sep=';', 
        header=True, 
        encoding='ISO-8859-1'
    )

    # Casting de colunas
    int_cols = [
        "NU_INSCRICAO", "TP_FAIXA_ETARIA", "TP_COR_RACA", "TP_ESCOLA", 
        "CO_MUNICIPIO_ESC", "CO_UF_ESC", "TP_DEPENDENCIA_ADM_ESC", 
        "CO_MUNICIPIO_PROVA", "CO_UF_PROVA", 
        "TP_PRESENCA_CN", "TP_PRESENCA_CH", "TP_PRESENCA_LC", "TP_PRESENCA_MT"
    ]
    for c in int_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast("long") if c == "NU_INSCRICAO" else col(c).cast("integer"))

    float_cols = ["NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO"]
    for c in float_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast("float"))

    # Calculando a média das notas (Substituindo nulos por 0 para presenças, ou apenas notas válidas)
    df = df.withColumn(
        "media_notas",
        (coalesce(col("NU_NOTA_CN"), lit(0.0)) + \
         coalesce(col("NU_NOTA_CH"), lit(0.0)) + \
         coalesce(col("NU_NOTA_LC"), lit(0.0)) + \
         coalesce(col("NU_NOTA_MT"), lit(0.0)) + \
         coalesce(col("NU_NOTA_REDACAO"), lit(0.0))) / 5.0
    )

    print("Construindo Dimensão Aluno...")
    dim_aluno = df.select(
        col("NU_INSCRICAO").alias("nu_inscricao"),
        col("TP_SEXO").alias("tp_sexo"),
        col("TP_FAIXA_ETARIA").alias("tp_faixa_etaria"),
        col("TP_COR_RACA").alias("tp_cor_raca")
    ).dropDuplicates(["nu_inscricao"]).na.drop(subset=["nu_inscricao"])

    print("Construindo Dimensão Escola...")
    dim_escola = df.select(
        col("CO_MUNICIPIO_ESC").alias("co_municipio_esc"),
        col("TP_ESCOLA").alias("tp_escola"),
        col("NO_MUNICIPIO_ESC").alias("no_municipio_esc"),
        col("CO_UF_ESC").alias("co_uf_esc"),
        col("SG_UF_ESC").alias("sg_uf_esc"),
        col("TP_DEPENDENCIA_ADM_ESC").alias("tp_dependencia_adm_esc")
    ).dropDuplicates(["co_municipio_esc"]).na.drop(subset=["co_municipio_esc"])

    print("Construindo Dimensão Localidade Prova...")
    dim_localidade = df.select(
        col("CO_MUNICIPIO_PROVA").alias("co_municipio_prova"),
        col("NO_MUNICIPIO_PROVA").alias("no_municipio_prova"),
        col("CO_UF_PROVA").alias("co_uf_prova"),
        col("SG_UF_PROVA").alias("sg_uf_prova")
    ).dropDuplicates(["co_municipio_prova"]).na.drop(subset=["co_municipio_prova"])

    print("Construindo Fato Resultados...")
    fato_resultados = df.select(
        col("NU_INSCRICAO").alias("nu_inscricao"),
        col("CO_MUNICIPIO_ESC").alias("co_municipio_esc"),
        col("CO_MUNICIPIO_PROVA").alias("co_municipio_prova"),
        col("TP_PRESENCA_CN").alias("tp_presenca_cn"),
        col("TP_PRESENCA_CH").alias("tp_presenca_ch"),
        col("TP_PRESENCA_LC").alias("tp_presenca_lc"),
        col("TP_PRESENCA_MT").alias("tp_presenca_mt"),
        col("NU_NOTA_CN").alias("nota_cn"),
        col("NU_NOTA_CH").alias("nota_ch"),
        col("NU_NOTA_LC").alias("nota_lc"),
        col("NU_NOTA_MT").alias("nota_mt"),
        col("NU_NOTA_REDACAO").alias("nota_redacao"),
        col("media_notas").alias("media_notas")
    ).na.drop(subset=["nu_inscricao"])

    # Escrevendo no MySQL
    def write_to_mysql(dataframe, table_name):
        print(f"Escrevendo na tabela {table_name}...")
        try:
            dataframe.write \
                .jdbc(url=JDBC_URL, table=table_name, mode="append", properties=JDBC_PROPERTIES)
            print(f"Tabela {table_name} carregada com sucesso!")
        except Exception as e:
            print(f"Erro ao escrever na tabela {table_name}: {e}")

    # Espera o BD subir completamente
    time.sleep(10)

    write_to_mysql(dim_aluno, "dim_aluno")
    write_to_mysql(dim_escola, "dim_escola")
    write_to_mysql(dim_localidade, "dim_localidade_prova")
    write_to_mysql(fato_resultados, "fato_resultados")

    print("Processo ETL Finalizado com Sucesso!")
    spark.stop()

if __name__ == "__main__":
    main()
