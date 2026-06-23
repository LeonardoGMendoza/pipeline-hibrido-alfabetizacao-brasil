# Databricks notebook source / PySpark Script Local
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

class SilverLayer:
    def __init__(self, bronze_dir='data/bronze/'):
        self.bronze_dir = bronze_dir
        # Configuração do Spark com gerenciamento de memória (FinOps local)
        self.spark = SparkSession.builder \
            .appName("Carga_Silver_Alfabetizacao_V2") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()

    def processar_silver(self):
        print("⚙️ [SILVER] Iniciando processamento PySpark - Limpeza e JOINs...")
        try:
            # 1. Leitura das 6 tabelas Bronze
            df_uf = self.spark.read.parquet(f"{self.bronze_dir}uf.parquet")
            df_mun = self.spark.read.parquet(f"{self.bronze_dir}municipio.parquet")
            df_meta_br = self.spark.read.parquet(f"{self.bronze_dir}meta_brasil.parquet")
            df_meta_uf = self.spark.read.parquet(f"{self.bronze_dir}meta_uf.parquet")
            df_meta_mun = self.spark.read.parquet(f"{self.bronze_dir}meta_municipio.parquet")
            df_alunos = self.spark.read.parquet(f"{self.bronze_dir}alunos.parquet")
            
            # 2. Limpeza e Padronização
            df_mun = df_mun.dropna(subset=['id_municipio']).withColumn("nome_municipio", F.upper(F.col("nome")))
            df_uf = df_uf.withColumn("nome_uf", F.upper(F.col("nome")))
            
            # 3. Integração (JOINs)
            # Cruza Municipio com UF
            df_geo = df_mun.join(df_uf, "sigla_uf", "left").drop(df_uf.nome)
            
            # Cruza Geografia com Metas do Municipio
            df_integrado = df_geo.join(df_meta_mun, "id_municipio", "inner")
            
            # Cruza com Dados dos Alunos
            df_integrado = df_integrado.join(df_alunos, "id_municipio", "left")
            
            # 4. Regras de Qualidade
            df_silver = df_integrado.dropDuplicates(["id_municipio", "ano", "rede"])
            df_silver = df_silver.withColumn("_data_ingestao_silver", F.current_timestamp())
            
            # 5. Persistência (Uso eficiente Parquet FinOps)
            os.makedirs('data/silver', exist_ok=True)
            output_path = 'data/silver/indicador_alfabetizacao_integrado.parquet'
            
            df_silver.write.mode("overwrite").parquet(output_path)
            
            print("✅ [SILVER] Camada Silver processada e integrada com sucesso.")
            return True
        except Exception as e:
            print(f"❌ Erro no processamento Silver: {e}")
            return False

if __name__ == '__main__':
    SilverLayer().processar_silver()
