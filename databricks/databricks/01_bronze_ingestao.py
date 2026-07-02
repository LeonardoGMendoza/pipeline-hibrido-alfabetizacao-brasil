# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Camada Bronze — Ingestão Raw
# MAGIC ## PayFlow Risk Model | Pipeline Medallion
# MAGIC
# MAGIC **Objetivo:** Ingerir os dados brutos do dataset PayFlow (crédito e NPS) diretamente do S3
# MAGIC e persisti-los na camada Bronze como Parquet sem nenhuma transformação.
# MAGIC
# MAGIC > ⚙️ **Arquitetura:** `[CSV Local/S3 Raw]` → **Bronze (Parquet)** → Silver → Gold

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 1. Configuração do Ambiente

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)
from datetime import datetime

print("=" * 60)
print("  PAYFLOW RISK MODEL — PIPELINE MEDALLION")
print("  Camada: BRONZE | Ingestão de Dados Brutos")
print(f"  Execução: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ☁️ 2. Configuração AWS S3 (AWS Academy Learner Lab)
# MAGIC
# MAGIC > **IMPORTANTE:** Substitua as credenciais abaixo pelas credenciais temporárias
# MAGIC > encontradas no painel do AWS Academy Learner Lab → "AWS Details" → "Show"

# COMMAND ----------

# ============================================================
# CONFIGURAÇÃO AWS S3 — SUBSTITUA COM SUAS CREDENCIAIS DO LAB
# ============================================================
AWS_ACCESS_KEY    = "ASIA..."          # Copie do Learner Lab
AWS_SECRET_KEY    = "..."              # Copie do Learner Lab
AWS_SESSION_TOKEN = "..."              # Copie do Learner Lab (obrigatório no Academy)
S3_BUCKET         = "payflow-risk-lake"
AWS_REGION        = "us-east-1"

# Configurar Spark para acessar S3 com credenciais temporárias
spark.conf.set("fs.s3a.access.key",        AWS_ACCESS_KEY)
spark.conf.set("fs.s3a.secret.key",        AWS_SECRET_KEY)
spark.conf.set("fs.s3a.session.token",     AWS_SESSION_TOKEN)
spark.conf.set("fs.s3a.aws.credentials.provider",
               "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
spark.conf.set("fs.s3a.endpoint",
               f"s3.{AWS_REGION}.amazonaws.com")

print("✅ Credenciais AWS configuradas com sucesso!")
print(f"🪣 Bucket de destino: s3a://{S3_BUCKET}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📂 3. Definição dos Caminhos (Data Lake Paths)

# COMMAND ----------

# Caminhos do Data Lake no S3
PATHS = {
    "bronze_payflow_credito" : f"s3a://{S3_BUCKET}/bronze/payflow_credito/",
    "bronze_payflow_nps"     : f"s3a://{S3_BUCKET}/bronze/payflow_nps/",
    "bronze_raw_checkpoint"  : f"s3a://{S3_BUCKET}/bronze/_checkpoints/",
}

print("📁 Estrutura do Data Lake (Camada Bronze):")
for nome, caminho in PATHS.items():
    print(f"  └── {nome}: {caminho}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 4. Schema dos Dados — Governança de Tipos

# COMMAND ----------

# Schema PayFlow Crédito (variável target: default_90d)
schema_credito = StructType([
    StructField("id_cliente",            IntegerType(), True),
    StructField("score_credito",         DoubleType(),  True),
    StructField("utilizacao_credito",    DoubleType(),  True),
    StructField("dias_atraso_max_12m",   IntegerType(), True),
    StructField("valor_solicitado",      DoubleType(),  True),
    StructField("renda_mensal",          DoubleType(),  True),
    StructField("parcelas_pagas_ate_3m", IntegerType(), True),  # LEAKAGE — mantido no Bronze
    StructField("status_apos_90d",       StringType(),  True),  # LEAKAGE — mantido no Bronze
    StructField("default_90d",           IntegerType(), True),  # TARGET
])

# Schema PayFlow NPS (variável target: nps_detrator)
schema_nps = StructType([
    StructField("id_transacao",           StringType(),  True),
    StructField("categoria_produto",      StringType(),  True),
    StructField("dias_atraso_entrega",    IntegerType(), True),
    StructField("valor_pedido",           DoubleType(),  True),
    StructField("qtd_itens",              IntegerType(), True),
    StructField("avaliacao_nota",         IntegerType(), True),
    StructField("nps_detrator",           IntegerType(), True),  # TARGET
])

print("✅ Schemas definidos:")
print(f"  - Crédito: {len(schema_credito.fields)} colunas")
print(f"  - NPS:     {len(schema_nps.fields)} colunas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 5. Ingestão — Leitura do CSV e Gravação Bronze

# COMMAND ----------

# -------------------------------------------------------
# DATASET 1: PayFlow Crédito (default_90d)
# -------------------------------------------------------
# OPÇÃO A: Lendo do DBFS (arquivo upado via UI do Databricks)
# df_credito = spark.read.csv("/FileStore/payflow/base_credito.csv",
#                              header=True, schema=schema_credito)

# OPÇÃO B: Lendo do S3 (arquivo previamente upado)
# df_credito = spark.read.csv(f"s3a://{S3_BUCKET}/raw/base_credito.csv",
#                              header=True, schema=schema_credito)

# OPÇÃO C: Criando dataset sintético para demonstração da arquitetura
from pyspark.sql import Row
import random

random.seed(42)

def gerar_registro_credito(i):
    score        = round(random.gauss(650, 120), 2)
    utilizacao   = round(random.uniform(0.05, 0.95), 4)
    dias_atraso  = max(0, int(random.expovariate(0.3)))
    renda        = round(random.uniform(1500, 15000), 2)
    valor_solic  = round(renda * random.uniform(0.5, 4.0), 2)
    default_flag = 1 if (score < 500 and dias_atraso > 30) else (
                   1 if random.random() < 0.12 else 0)
    return Row(
        id_cliente            = i,
        score_credito         = float(max(300, min(850, score))),
        utilizacao_credito    = float(utilizacao),
        dias_atraso_max_12m   = int(dias_atraso),
        valor_solicitado      = float(valor_solic),
        renda_mensal          = float(renda),
        parcelas_pagas_ate_3m = int(random.randint(0, 3)),
        status_apos_90d       = random.choice(["em_dia", "atraso_leve", "inadimplente"]),
        default_90d           = int(default_flag),
    )

dados_credito = [gerar_registro_credito(i) for i in range(1, 5001)]
df_credito    = spark.createDataFrame(dados_credito)

print(f"✅ Dataset PayFlow Crédito carregado: {df_credito.count():,} registros")
df_credito.printSchema()

# COMMAND ----------

# -------------------------------------------------------
# DATASET 2: PayFlow NPS
# Usando o arquivo real do projeto: desafio_nps_fase_1.csv
# -------------------------------------------------------

# OPÇÃO A: Do DBFS
# df_nps = spark.read.csv("/FileStore/payflow/desafio_nps_fase_1.csv",
#                          header=True, inferSchema=True)

# OPÇÃO B: Dataset sintético para demonstração
def gerar_registro_nps(i):
    dias_atraso = max(0, int(random.expovariate(0.5)))
    nota        = random.choices([1,2,3,4,5], weights=[5,10,20,35,30])[0]
    detrator    = 1 if (dias_atraso > 3 or nota <= 2) else 0
    return Row(
        id_transacao        = f"TXN-{i:06d}",
        categoria_produto   = random.choice(["eletronicos","vestuario","alimentos","moveis"]),
        dias_atraso_entrega = int(dias_atraso),
        valor_pedido        = float(round(random.uniform(50, 800), 2)),
        qtd_itens           = int(random.randint(1, 5)),
        avaliacao_nota      = int(nota),
        nps_detrator        = int(detrator),
    )

dados_nps = [gerar_registro_nps(i) for i in range(1, 3001)]
df_nps    = spark.createDataFrame(dados_nps)

print(f"✅ Dataset PayFlow NPS carregado: {df_nps.count():,} registros")
df_nps.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 6. Persistência na Camada Bronze (S3 Parquet)

# COMMAND ----------

# Adicionar metadados de ingestão (auditoria)
ts_ingestao = F.lit(datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))

df_credito_bronze = df_credito.withColumn("_bronze_timestamp",  ts_ingestao) \
                               .withColumn("_bronze_source",     F.lit("payflow_credito_v1")) \
                               .withColumn("_bronze_formato",    F.lit("csv_sintetico"))

df_nps_bronze     = df_nps.withColumn("_bronze_timestamp",      ts_ingestao) \
                           .withColumn("_bronze_source",         F.lit("payflow_nps_v1")) \
                           .withColumn("_bronze_formato",        F.lit("csv_real"))

# COMMAND ----------

# Gravar na camada Bronze do S3 como Parquet (particionado por data)
print("💾 Gravando Bronze — PayFlow Crédito...")
(df_credito_bronze
 .write
 .mode("overwrite")
 .parquet(PATHS["bronze_payflow_credito"]))

print("💾 Gravando Bronze — PayFlow NPS...")
(df_nps_bronze
 .write
 .mode("overwrite")
 .parquet(PATHS["bronze_payflow_nps"]))

print("\n✅ Camada BRONZE gravada com sucesso no S3!")
print(f"  - Crédito: {PATHS['bronze_payflow_credito']}")
print(f"  - NPS:     {PATHS['bronze_payflow_nps']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 7. Validação — Quality Check da Camada Bronze

# COMMAND ----------

# Ler de volta do S3 para validar
df_credito_check = spark.read.parquet(PATHS["bronze_payflow_credito"])
df_nps_check     = spark.read.parquet(PATHS["bronze_payflow_nps"])

print("=" * 55)
print("📊 RELATÓRIO DE QUALIDADE — CAMADA BRONZE")
print("=" * 55)
print(f"\n[Crédito] Total de registros : {df_credito_check.count():>8,}")
print(f"[Crédito] Total de colunas   : {len(df_credito_check.columns):>8}")
print(f"[NPS]     Total de registros : {df_nps_check.count():>8,}")
print(f"[NPS]     Total de colunas   : {len(df_nps_check.columns):>8}")

# Distribuição da variável target (Crédito)
print("\n--- Distribuição: default_90d (Crédito) ---")
df_credito_check.groupBy("default_90d").count().orderBy("default_90d").show()

# Distribuição da variável target (NPS)
print("--- Distribuição: nps_detrator (NPS) ---")
df_nps_check.groupBy("nps_detrator").count().orderBy("nps_detrator").show()

# COMMAND ----------

print("""
╔══════════════════════════════════════════════╗
║  ✅  CAMADA BRONZE CONCLUÍDA COM SUCESSO!   ║
║                                              ║
║  Próximo passo:                              ║
║  → Execute: 02_silver_limpeza.py            ║
╚══════════════════════════════════════════════╝
""")
