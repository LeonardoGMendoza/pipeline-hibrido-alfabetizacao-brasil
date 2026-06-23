# Databricks notebook source / PySpark Script Local
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

class SilverLayer:
    def __init__(self, bronze_path='data/bronze/indicador_alfabetizacao_raw.parquet'):
        self.bronze_path = bronze_path
        self.spark = SparkSession.builder.appName("Carga_Silver_Alfabetizacao").getOrCreate()

    def processar_silver(self):
        print("Iniciando motor PySpark - Camada Silver...")
        try:
            # Leitura da Camada Bronze
            df_bronze = self.spark.read.parquet(self.bronze_path)
            
            # 1. Padronização de Colunas
            colunas_padrao = [F.col(c).alias(c.lower().strip()) for c in df_bronze.columns]
            df_silver = df_bronze.select(*colunas_padrao)
            
            # 2. Remoção de Nulos Críticos (Registros sem nota do SAEB)
            df_silver = df_silver.dropna(subset=['indicador_alfabetizacao'])
            
            # 3. Tipagem de Dados e Limpeza de Strings
            df_silver = df_silver.withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
            df_silver = df_silver.withColumn("rede", F.initcap(F.trim(F.col("rede"))))
            
            # 4. Remoção de Duplicados Baseados na Chave Primária
            chaves = ['ano', 'id_municipio', 'rede', 'localizacao']
            df_silver = df_silver.dropDuplicates(subset=chaves)
            
            # 5. Adiciona Rastreabilidade
            df_silver = df_silver.withColumn("_data_processamento", F.current_timestamp())
            
            # Persistência
            os.makedirs('data/silver', exist_ok=True)
            output_path = 'data/silver/indicador_alfabetizacao_silver.parquet'
            
            # Em produção usaríamos overwrite ou append baseado no particionamento
            df_silver.write.mode("overwrite").parquet(output_path)
            
            print(f"Camada Silver processada com sucesso no Spark.")
            return True
        except Exception as e:
            print(f"Erro no processamento Silver: {e}")
            return False

if __name__ == '__main__':
    # Se rodar localmente para testes:
    # Crie um Parquet fake de bronze se não existir
    os.makedirs('data/bronze', exist_ok=True)
    if not os.path.exists('data/bronze/indicador_alfabetizacao_raw.parquet'):
        import pandas as pd
        pd.DataFrame({
            'ano': [2023, 2023], 'sigla_uf': ['SP', 'RJ'], 
            'id_municipio': [3550308, 3304557], 'rede': ['Estadual', 'Municipal'],
            'localizacao': ['Urbana', 'Rural'], 'indicador_alfabetizacao': [80.5, 74.3]
        }).to_parquet('data/bronze/indicador_alfabetizacao_raw.parquet', index=False)
        
    SilverLayer().processar_silver()
