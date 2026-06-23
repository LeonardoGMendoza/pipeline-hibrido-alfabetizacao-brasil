# Databricks notebook source / PySpark Script Local
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

class GoldLayer:
    def __init__(self, silver_path='data/silver/indicador_alfabetizacao_silver.parquet'):
        self.silver_path = silver_path
        self.spark = SparkSession.builder.appName("Carga_Gold_Alfabetizacao").getOrCreate()

    def processar_gold(self):
        print("Iniciando motor PySpark - Camada Gold (Agregações)...")
        try:
            # Leitura da Camada Silver
            df_silver = self.spark.read.parquet(self.silver_path)
            
            # 1. Regra de Negócio Padrão do SAEB (743 pontos)
            # Definir quem atingiu a meta
            df_gold = df_silver.withColumn(
                "status_alfabetizacao",
                F.when(F.col("indicador_alfabetizacao") >= 743, "Alfabetizado (≥ 743 pts)")
                 .otherwise("Atenção (< 743 pts)")
            )
            
            # 2. Agregação Analítica Nível 1: Visão por Estado
            # Aqui poderíamos cruzar com uma tabela de metas, mas vamos simplificar o motor
            df_estado = df_gold.groupBy("ano", "sigla_uf") \
                .agg(
                    F.round(F.avg("indicador_alfabetizacao"), 2).alias("proficiencia_media"),
                    F.count("id_municipio").alias("qtd_municipios_valiados")
                )
                
            # O dataframe principal que vai pro dashboard (ou Caio/MongoDB) será o df_gold
            # com colunas selecionadas para facilitar.
            
            # Persistência do Cubo Principal
            os.makedirs('data/gold', exist_ok=True)
            output_path = 'data/gold/indicador_alfabetizacao.parquet'
            
            # Em Pandas salvaríamos CSV, no Spark salvamos Parquet para o NoSQL ler
            df_gold.write.mode("overwrite").parquet(output_path)
            
            print(f"Camada Gold processada com sucesso no Spark.")
            return True
        except Exception as e:
            print(f"Erro no processamento Gold: {e}")
            return False

if __name__ == '__main__':
    GoldLayer().processar_gold()
