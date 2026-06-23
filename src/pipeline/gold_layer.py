from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

class GoldLayer:
    def __init__(self, silver_path='data/silver/indicador_alfabetizacao_integrado.parquet'):
        self.silver_path = silver_path
        self.spark = SparkSession.builder.appName("Carga_Gold_Alfabetizacao_DadosReais").getOrCreate()

    def processar_gold(self):
        print("🏆 [GOLD] Processando Agregações Finais com Dados Reais...")
        try:
            df_silver = self.spark.read.parquet(self.silver_path)
            
            # Como agora temos dados reais (PC_ALUNO_ALFABETIZADO), vamos usar eles
            # Caso a proficiência (VL_MEDIA_LP) >= 743, marcamos a meta como atingida
            
            df_gold = df_silver.withColumn(
                "status_alfabetizacao",
                F.when(F.col("VL_MEDIA_LP") >= 743.0, "Meta Atingida (≥ 743 pts SAEB)")
                 .otherwise("Atenção (< 743 pts SAEB)")
            )
            
            # Seleciona as colunas formatadas para o Dashboard do Reinaldo e o MongoDB do Caio
            df_analitico = df_gold.select(
                F.col("NO_MUNICIPIO").alias("nome_municipio"),
                F.col("CO_MUNICIPIO").alias("id_municipio"),
                F.col("SG_UF").alias("sigla_uf"),
                F.col("PC_ALUNO_ALFABETIZADO").alias("taxa_alfabetizacao"),
                F.col("qtd_alunos_avaliados"),
                F.col("status_alfabetizacao")
            ).dropDuplicates(["id_municipio"])
            
            # Preenchendo nulos eventuais em qtd de alunos
            df_analitico = df_analitico.fillna({"qtd_alunos_avaliados": 0})
            
            # Adicionando mock de vulnerabilidade caso a base real do Inep não tenha
            df_analitico = df_analitico.withColumn("vulnerabilidade_social", F.lit("Não Informado"))
            
            os.makedirs('data/gold', exist_ok=True)
            output_path = 'data/gold/indicador_alfabetizacao.parquet'
            
            df_analitico.write.mode("overwrite").parquet(output_path)
            print("✅ [GOLD] Camada Gold finalizada com os DADOS REAIS! Pronta para Streamlit e MongoDB.")
            return True
        except Exception as e:
            print(f"❌ Erro no processamento Gold: {e}")
            return False

if __name__ == '__main__':
    GoldLayer().processar_gold()
