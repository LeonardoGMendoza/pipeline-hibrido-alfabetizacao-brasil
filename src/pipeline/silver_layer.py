from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

class SilverLayer:
    def __init__(self, bronze_dir='data/bronze/'):
        self.bronze_dir = bronze_dir
        self.spark = SparkSession.builder \
            .appName("Carga_Silver_Alfabetizacao_DadosReais") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()

    def processar_silver(self):
        print("⚙️ [SILVER] Processando dados REAIS de microdados do INEP...")
        try:
            # 1. Leitura das tabelas Parquet da Bronze (Convertidas do Excel/CSV)
            df_aluno = self.spark.read.parquet(f"{self.bronze_dir}TS_ALUNO.parquet")
            df_mun = self.spark.read.parquet(f"{self.bronze_dir}TS_MUNICIPIO.parquet")
            df_est = self.spark.read.parquet(f"{self.bronze_dir}TS_ESTADO.parquet")
            df_metas_mun = self.spark.read.parquet(f"{self.bronze_dir}metas_municipios.parquet")
            
            # 2. Limpeza Básica nos Microdados de Alunos
            # Filtra apenas alunos que realizaram a prova (IN_PRESENCA_LP == 1)
            df_aluno_valido = df_aluno.filter(F.col("IN_PRESENCA_LP") == 1)
            
            # Agrega proficiência dos alunos por município para bater com a tabela de metas
            df_aluno_agg = df_aluno_valido.groupBy("CO_MUNICIPIO").agg(
                F.avg("VL_PROFICIENCIA_LP").alias("proficiencia_media_alunos"),
                F.count("ID_ALUNO").alias("qtd_alunos_avaliados")
            )
            
            # 3. Integração (JOINs) com Municípios e Metas
            # O Excel de Metas tem a coluna 'Município' (nome) ou possivelmente um código.
            # Como vimos no TS_MUNICIPIO, existe o 'CO_MUNICIPIO'. Vamos cruzar.
            
            df_integrado = df_mun.join(df_aluno_agg, "CO_MUNICIPIO", "left")
            
            # Regras de Qualidade
            df_silver = df_integrado.withColumn("_data_ingestao_silver", F.current_timestamp())
            
            # 5. Persistência
            os.makedirs('data/silver', exist_ok=True)
            output_path = 'data/silver/indicador_alfabetizacao_integrado.parquet'
            
            df_silver.write.mode("overwrite").parquet(output_path)
            
            print("✅ [SILVER] Camada Silver processada com os DADOS REAIS!")
            return True
        except Exception as e:
            print(f"❌ Erro no processamento Silver: {e}")
            return False

if __name__ == '__main__':
    SilverLayer().processar_silver()
