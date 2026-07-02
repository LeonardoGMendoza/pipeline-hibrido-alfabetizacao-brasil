# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Camada Silver — Limpeza & Feature Engineering
# MAGIC ## PayFlow Risk Model | Pipeline Medallion
# MAGIC
# MAGIC **Objetivo:** Ler a camada Bronze, aplicar limpeza, remover data leakage,
# MAGIC criar features de engenharia e persistir na camada Silver (Parquet limpo).
# MAGIC
# MAGIC > ⚙️ **Arquitetura:** Bronze → **Silver (Parquet Limpo)** → Gold

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window
from datetime import datetime

print("=" * 60)
print("  PAYFLOW RISK MODEL — PIPELINE MEDALLION")
print("  Camada: SILVER | Limpeza + Feature Engineering")
print(f"  Execução: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ☁️ 1. Configuração AWS S3

# COMMAND ----------

AWS_ACCESS_KEY    = "ASIA..."          # Copie do Learner Lab
AWS_SECRET_KEY    = "..."              # Copie do Learner Lab
AWS_SESSION_TOKEN = "..."              # Copie do Learner Lab
S3_BUCKET         = "payflow-risk-lake"
AWS_REGION        = "us-east-1"

spark.conf.set("fs.s3a.access.key",        AWS_ACCESS_KEY)
spark.conf.set("fs.s3a.secret.key",        AWS_SECRET_KEY)
spark.conf.set("fs.s3a.session.token",     AWS_SESSION_TOKEN)
spark.conf.set("fs.s3a.aws.credentials.provider",
               "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
spark.conf.set("fs.s3a.endpoint",
               f"s3.{AWS_REGION}.amazonaws.com")

PATHS = {
    "bronze_credito" : f"s3a://{S3_BUCKET}/bronze/payflow_credito/",
    "bronze_nps"     : f"s3a://{S3_BUCKET}/bronze/payflow_nps/",
    "silver_credito" : f"s3a://{S3_BUCKET}/silver/payflow_credito/",
    "silver_nps"     : f"s3a://{S3_BUCKET}/silver/payflow_nps/",
}

print("✅ Configuração concluída.")
print(f"📁 Lendo Bronze de: s3a://{S3_BUCKET}/bronze/")
print(f"📁 Gravando Silver em: s3a://{S3_BUCKET}/silver/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📥 2. Leitura da Camada Bronze

# COMMAND ----------

df_bronze_credito = spark.read.parquet(PATHS["bronze_credito"])
df_bronze_nps     = spark.read.parquet(PATHS["bronze_nps"])

print(f"✅ Bronze Crédito: {df_bronze_credito.count():,} registros | {len(df_bronze_credito.columns)} colunas")
print(f"✅ Bronze NPS:     {df_bronze_nps.count():,} registros | {len(df_bronze_nps.columns)} colunas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 3. Análise de Nulos — Diagnóstico

# COMMAND ----------

print("📊 ANÁLISE DE NULOS — PayFlow Crédito:")
print("-" * 45)
for col_name in df_bronze_credito.columns:
    null_count = df_bronze_credito.filter(F.col(col_name).isNull()).count()
    pct        = (null_count / df_bronze_credito.count()) * 100
    status     = "⚠️ NULO" if null_count > 0 else "✅ OK"
    print(f"  {status} | {col_name:<30} | {null_count:>5} ({pct:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 4. Limpeza — PayFlow Crédito

# COMMAND ----------

# ============================================================
# PASSO 1: Remover variáveis com Data Leakage
# CRISP-DM: Preparação de Dados — eliminação de vazamento
# ============================================================
COLUNAS_LEAKAGE   = ["parcelas_pagas_ate_3m", "status_apos_90d"]
COLUNAS_METADATA  = [c for c in df_bronze_credito.columns if c.startswith("_bronze_")]

df_silver_credito = df_bronze_credito.drop(*COLUNAS_LEAKAGE, *COLUNAS_METADATA)

print(f"✅ Leakage removido: {COLUNAS_LEAKAGE}")

# ============================================================
# PASSO 2: Imputação de Nulos — Mediana (robusto a outliers)
# ============================================================
# Calcular medianas
mediana_score      = df_silver_credito.approxQuantile("score_credito",      [0.5], 0.01)[0]
mediana_utilizacao = df_silver_credito.approxQuantile("utilizacao_credito", [0.5], 0.01)[0]
mediana_atraso     = df_silver_credito.approxQuantile("dias_atraso_max_12m",[0.5], 0.01)[0]
mediana_renda      = df_silver_credito.approxQuantile("renda_mensal",       [0.5], 0.01)[0]
mediana_valor      = df_silver_credito.approxQuantile("valor_solicitado",   [0.5], 0.01)[0]

df_silver_credito = (df_silver_credito
    .fillna({"score_credito":      mediana_score})
    .fillna({"utilizacao_credito": mediana_utilizacao})
    .fillna({"dias_atraso_max_12m":int(mediana_atraso)})
    .fillna({"renda_mensal":       mediana_renda})
    .fillna({"valor_solicitado":   mediana_valor})
)

print(f"✅ Imputação por mediana aplicada.")

# ============================================================
# PASSO 3: Feature Engineering
# Nova variável: comprometimento_renda = valor_solicitado / renda_mensal
# ============================================================
df_silver_credito = df_silver_credito.withColumn(
    "comprometimento_renda",
    F.round(F.col("valor_solicitado") / F.col("renda_mensal"), 4)
)

# ============================================================
# PASSO 4: Segmentação de Risco (feature categórica)
# ============================================================
df_silver_credito = df_silver_credito.withColumn(
    "faixa_score",
    F.when(F.col("score_credito") >= 750, "BAIXO_RISCO")
     .when(F.col("score_credito") >= 600, "MEDIO_RISCO")
     .when(F.col("score_credito") >= 450, "ALTO_RISCO")
     .otherwise("CRITICO")
)

df_silver_credito = df_silver_credito.withColumn(
    "flag_atraso_critico",
    F.when(F.col("dias_atraso_max_12m") > 30, 1).otherwise(0)
)

# ============================================================
# PASSO 5: Filtro de qualidade — remover registros inválidos
# ============================================================
registros_antes = df_silver_credito.count()

df_silver_credito = df_silver_credito.filter(
    (F.col("renda_mensal")       > 0) &
    (F.col("score_credito")      >= 300) &
    (F.col("score_credito")      <= 850) &
    (F.col("utilizacao_credito") >= 0) &
    (F.col("utilizacao_credito") <= 1)
)

registros_depois = df_silver_credito.count()
print(f"✅ Filtro de qualidade: {registros_antes:,} → {registros_depois:,} registros")
print(f"   Removidos: {registros_antes - registros_depois:,} registros inválidos")

# Adicionar metadados Silver
df_silver_credito = df_silver_credito \
    .withColumn("_silver_timestamp", F.lit(datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))) \
    .withColumn("_silver_versao",    F.lit("v1.0"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 5. Limpeza — PayFlow NPS

# COMMAND ----------

COLUNAS_METADATA_NPS = [c for c in df_bronze_nps.columns if c.startswith("_bronze_")]
df_silver_nps = df_bronze_nps.drop(*COLUNAS_METADATA_NPS)

# Remover nulos
df_silver_nps = df_silver_nps.dropna(subset=["avaliacao_nota", "nps_detrator"])

# Feature: flag de atraso crítico (ruptura logística > 3 dias)
df_silver_nps = df_silver_nps.withColumn(
    "flag_atraso_critico",
    F.when(F.col("dias_atraso_entrega") > 3, 1).otherwise(0)
)

# Feature: faixa de valor do pedido
df_silver_nps = df_silver_nps.withColumn(
    "faixa_valor",
    F.when(F.col("valor_pedido") > 500, "ALTO")
     .when(F.col("valor_pedido") > 200, "MEDIO")
     .otherwise("BAIXO")
)

# Metadados
df_silver_nps = df_silver_nps \
    .withColumn("_silver_timestamp", F.lit(datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))) \
    .withColumn("_silver_versao",    F.lit("v1.0"))

print(f"✅ Silver NPS preparado: {df_silver_nps.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 6. Persistência na Camada Silver (S3)

# COMMAND ----------

print("💾 Gravando Silver — PayFlow Crédito no S3...")
(df_silver_credito
 .write
 .mode("overwrite")
 .partitionBy("faixa_score")
 .parquet(PATHS["silver_credito"]))

print("💾 Gravando Silver — PayFlow NPS no S3...")
(df_silver_nps
 .write
 .mode("overwrite")
 .partitionBy("categoria_produto")
 .parquet(PATHS["silver_nps"]))

print("\n✅ Camada SILVER gravada com sucesso no S3!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 7. Relatório de Qualidade — Silver

# COMMAND ----------

df_silver_credito_check = spark.read.parquet(PATHS["silver_credito"])
df_silver_nps_check     = spark.read.parquet(PATHS["silver_nps"])

print("=" * 60)
print("📊 RELATÓRIO DE QUALIDADE — CAMADA SILVER")
print("=" * 60)

print(f"\n[Crédito] Total de registros : {df_silver_credito_check.count():>8,}")
print(f"[Crédito] Total de colunas   : {len(df_silver_credito_check.columns):>8}")
print(f"[NPS]     Total de registros : {df_silver_nps_check.count():>8,}")
print(f"[NPS]     Total de colunas   : {len(df_silver_nps_check.columns):>8}")

print("\n--- Distribuição por Faixa de Score (Crédito) ---")
df_silver_credito_check.groupBy("faixa_score") \
    .agg(
        F.count("*").alias("total"),
        F.round(F.mean("default_90d") * 100, 2).alias("taxa_default_pct"),
        F.round(F.mean("comprometimento_renda"), 3).alias("comprometimento_medio")
    ).orderBy("faixa_score").show()

print("--- Feature: comprometimento_renda ---")
df_silver_credito_check.select(
    F.round(F.min("comprometimento_renda"),  3).alias("min"),
    F.round(F.mean("comprometimento_renda"), 3).alias("media"),
    F.round(F.max("comprometimento_renda"),  3).alias("max"),
).show()

# COMMAND ----------

print("""
╔══════════════════════════════════════════════╗
║  ✅  CAMADA SILVER CONCLUÍDA COM SUCESSO!   ║
║                                              ║
║  Leakage removido ✅                         ║
║  Nulos imputados  ✅                         ║
║  Features criadas ✅                         ║
║  Particionado     ✅                         ║
║                                              ║
║  Próximo passo:                              ║
║  → Execute: 03_gold_analitico.py            ║
╚══════════════════════════════════════════════╝
""")
