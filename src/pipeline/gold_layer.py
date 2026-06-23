from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

class GoldLayer:
    def __init__(self, silver_path='data/silver/indicador_alfabetizacao_integrado.parquet'):
        self.silver_path = silver_path
        self.spark = SparkSession.builder.appName("Carga_Gold_Alfabetizacao_V2").getOrCreate()

    def processar_gold(self):
        print("🏆 [GOLD] Iniciando agregação final...")
        try:
            df_silver = self.spark.read.parquet(self.silver_path)
            
            # Aplica Regra de Negócio: O Indicador Nacional é 743 pts
            # Como a tabela da BD já traz taxa_alfabetizacao, validamos se a meta foi batida
            
            df_gold = df_silver.withColumn(
                "status_alfabetizacao",
                F.when(F.col("taxa_alfabetizacao") >= 74.3, "Meta Atingida (≥ 743 pts SAEB)")
                 .otherwise("Atenção (< 743 pts SAEB)")
            )
            
            # Agregações Analíticas para o Dashboard e IA
            df_analitico = df_gold.select(
                F.col("nome_municipio").alias("municipio"),
                F.col("sigla_uf").alias("estado"),
                F.col("taxa_alfabetizacao").alias("proficiencia_media"),
                "qtd_alunos_avaliados",
                "vulnerabilidade_social",
                "status_alfabetizacao"
            )
            
            os.makedirs('data/gold', exist_ok=True)
            output_path = 'data/gold/indicador_alfabetizacao.parquet'
            
            df_analitico.write.mode("overwrite").parquet(output_path)
            print("✅ [GOLD] Camada Gold finalizada! Dados prontos para Streamlit e MongoDB.")
            return True
        except Exception as e:
            print(f"❌ Erro no processamento Gold: {e}")
            return False

if __name__ == '__main__':
    GoldLayer().processar_gold()
