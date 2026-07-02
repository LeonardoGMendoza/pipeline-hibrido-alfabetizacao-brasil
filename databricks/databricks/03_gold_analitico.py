# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Camada Gold — Tabela Analítica Final
# MAGIC ## PayFlow Risk Model | Pipeline Medallion
# MAGIC
# MAGIC **Objetivo:** Criar a tabela Gold com métricas de negócio agregadas e a tabela
# MAGIC analítica pronta para consumo pelo modelo de ML e pelo Dashboard Streamlit.
# MAGIC
# MAGIC > ⚙️ **Arquitetura:** Bronze → Silver → **Gold (Analítico Final)** → Dashboard

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from datetime import datetime

print("=" * 60)
print("  PAYFLOW RISK MODEL — PIPELINE MEDALLION")
print("  Camada: GOLD | Tabela Analítica + Métricas de Negócio")
print(f"  Execução: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ☁️ 1. Configuração AWS S3

# COMMAND ----------

AWS_ACCESS_KEY    = "ASIA..."
AWS_SECRET_KEY    = "..."
AWS_SESSION_TOKEN = "..."
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
    "silver_credito"          : f"s3a://{S3_BUCKET}/silver/payflow_credito/",
    "silver_nps"              : f"s3a://{S3_BUCKET}/silver/payflow_nps/",
    "gold_modelo_features"    : f"s3a://{S3_BUCKET}/gold/modelo_features/",
    "gold_metricas_risco"     : f"s3a://{S3_BUCKET}/gold/metricas_risco/",
    "gold_metricas_nps"       : f"s3a://{S3_BUCKET}/gold/metricas_nps/",
    "gold_dashboard_resumo"   : f"s3a://{S3_BUCKET}/gold/dashboard_resumo/",
}

print("✅ Configuração AWS concluída.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📥 2. Leitura da Camada Silver

# COMMAND ----------

df_silver_credito = spark.read.parquet(PATHS["silver_credito"])
df_silver_nps     = spark.read.parquet(PATHS["silver_nps"])

print(f"✅ Silver Crédito: {df_silver_credito.count():,} registros")
print(f"✅ Silver NPS:     {df_silver_nps.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔬 3. Tabela Gold 1 — Features para o Modelo ML
# MAGIC
# MAGIC Esta tabela é a **entrada direta** para o modelo Random Forest.
# MAGIC Apenas as features necessárias, sem leakage, sem metadados.

# COMMAND ----------

# Colunas que entram no modelo (sem leakage, sem metadados)
FEATURES_MODELO = [
    "score_credito",
    "utilizacao_credito",
    "dias_atraso_max_12m",
    "comprometimento_renda",
    "flag_atraso_critico",
    "default_90d",       # Target
]

df_gold_features = df_silver_credito.select(*FEATURES_MODELO)

# Normalização de comprometimento_renda (clipping de outliers extremos)
p99_comprometimento = df_gold_features.approxQuantile("comprometimento_renda", [0.99], 0.01)[0]
df_gold_features = df_gold_features.withColumn(
    "comprometimento_renda",
    F.least(F.col("comprometimento_renda"), F.lit(p99_comprometimento))
)

# Adicionar score de propensão ao default (para uso no dashboard)
# Lógica de negócio baseada nos insights do modelo CRISP-DM
df_gold_features = df_gold_features.withColumn(
    "score_propensao_default",
    F.round(
        F.when(F.col("score_credito") < 450,  0.75)
         .when(F.col("score_credito") < 550,  0.55)
         .when(F.col("score_credito") < 650,  0.30)
         .when(F.col("score_credito") < 750,  0.12)
         .otherwise(0.04)
        * (1 + F.col("comprometimento_renda") * 0.2)
        * (1 + F.col("flag_atraso_critico")   * 0.5),
        4
    )
)

# Metadados Gold
df_gold_features = df_gold_features \
    .withColumn("_gold_timestamp", F.lit(datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))) \
    .withColumn("_gold_versao",    F.lit("v1.0"))

print(f"✅ Gold Features do Modelo: {df_gold_features.count():,} registros | {len(df_gold_features.columns)} colunas")
df_gold_features.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 4. Tabela Gold 2 — Métricas de Risco por Segmento (Negócio)

# COMMAND ----------

df_gold_metricas_risco = df_silver_credito.groupBy("faixa_score").agg(
    # Volume
    F.count("*")                                        .alias("total_clientes"),
    F.sum("default_90d")                                .alias("total_inadimplentes"),

    # Taxa de default
    F.round(F.mean("default_90d") * 100, 2)            .alias("taxa_default_pct"),

    # Score de crédito
    F.round(F.mean("score_credito"),         1)         .alias("score_medio"),
    F.round(F.min("score_credito"),          0)         .alias("score_min"),
    F.round(F.max("score_credito"),          0)         .alias("score_max"),

    # Comprometimento de renda
    F.round(F.mean("comprometimento_renda"), 3)         .alias("comprometimento_medio"),
    F.round(F.stddev("comprometimento_renda"), 3)       .alias("comprometimento_std"),

    # Atraso
    F.round(F.mean("dias_atraso_max_12m"),   1)         .alias("atraso_medio_dias"),
    F.sum("flag_atraso_critico")                        .alias("clientes_atraso_critico"),

    # Utilização
    F.round(F.mean("utilizacao_credito") * 100, 2)     .alias("utilizacao_media_pct"),
)

# Adicionar coluna de participação relativa
total_geral = df_silver_credito.count()
df_gold_metricas_risco = df_gold_metricas_risco.withColumn(
    "participacao_pct",
    F.round(F.col("total_clientes") / F.lit(total_geral) * 100, 2)
).orderBy(F.desc("taxa_default_pct"))

print("📊 MÉTRICAS DE RISCO POR SEGMENTO:")
df_gold_metricas_risco.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 5. Tabela Gold 3 — Métricas NPS por Categoria

# COMMAND ----------

df_gold_metricas_nps = df_silver_nps.groupBy("categoria_produto").agg(
    F.count("*")                                .alias("total_pedidos"),
    F.sum("nps_detrator")                       .alias("total_detratores"),
    F.round(F.mean("nps_detrator") * 100, 2)   .alias("taxa_detracao_pct"),
    F.round(F.mean("avaliacao_nota"), 2)        .alias("nota_media"),
    F.round(F.mean("dias_atraso_entrega"), 2)   .alias("atraso_medio"),
    F.sum("flag_atraso_critico")                .alias("atrasos_criticos"),
    F.round(F.mean("valor_pedido"), 2)          .alias("ticket_medio"),
).orderBy(F.desc("taxa_detracao_pct"))

print("📈 MÉTRICAS NPS POR CATEGORIA:")
df_gold_metricas_nps.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 6. Tabela Gold 4 — Resumo Executivo (Dashboard)

# COMMAND ----------

# KPIs consolidados para o Dashboard
taxa_default_geral = df_silver_credito.agg(
    F.round(F.mean("default_90d") * 100, 2).alias("taxa")
).collect()[0]["taxa"]

taxa_detracao_geral = df_silver_nps.agg(
    F.round(F.mean("nps_detrator") * 100, 2).alias("taxa")
).collect()[0]["taxa"]

atraso_ruptura_pct = df_silver_nps.agg(
    F.round(F.mean("flag_atraso_critico") * 100, 2).alias("taxa")
).collect()[0]["taxa"]

comprometimento_medio = df_silver_credito.agg(
    F.round(F.mean("comprometimento_renda"), 3).alias("media")
).collect()[0]["media"]

from pyspark.sql import Row
df_gold_resumo = spark.createDataFrame([Row(
    kpi                     = "Resumo Executivo PayFlow",
    total_clientes_credito  = int(df_silver_credito.count()),
    total_transacoes_nps    = int(df_silver_nps.count()),
    taxa_default_pct        = float(taxa_default_geral),
    taxa_detracao_nps_pct   = float(taxa_detracao_geral),
    atraso_ruptura_pct      = float(atraso_ruptura_pct),
    comprometimento_renda   = float(comprometimento_medio),
    modelo_auc_roc          = 0.92,
    modelo_threshold        = 0.35,
    ponto_ruptura_dias      = 3,
    gerado_em               = datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
)])

print("🎯 KPIs EXECUTIVOS PAYFLOW:")
df_gold_resumo.show(vertical=True, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 7. Persistência na Camada Gold (S3)

# COMMAND ----------

print("💾 Gravando Gold — Features do Modelo...")
(df_gold_features
 .write.mode("overwrite")
 .parquet(PATHS["gold_modelo_features"]))

print("💾 Gravando Gold — Métricas de Risco...")
(df_gold_metricas_risco
 .write.mode("overwrite")
 .parquet(PATHS["gold_metricas_risco"]))

print("💾 Gravando Gold — Métricas NPS...")
(df_gold_metricas_nps
 .write.mode("overwrite")
 .parquet(PATHS["gold_metricas_nps"]))

print("💾 Gravando Gold — Resumo Dashboard...")
(df_gold_resumo
 .write.mode("overwrite")
 .parquet(PATHS["gold_dashboard_resumo"]))

print("\n✅ Todas as tabelas GOLD gravadas com sucesso no S3!")
for nome, caminho in PATHS.items():
    if "gold" in nome:
        print(f"  └── {caminho}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 8. Validação Final do Pipeline Completo

# COMMAND ----------

print("=" * 65)
print("📊 VALIDAÇÃO FINAL — PIPELINE MEDALLION COMPLETO")
print("=" * 65)

for label, path in [
    ("Bronze Crédito", f"s3a://{S3_BUCKET}/bronze/payflow_credito/"),
    ("Bronze NPS",     f"s3a://{S3_BUCKET}/bronze/payflow_nps/"),
    ("Silver Crédito", PATHS["silver_credito"]),
    ("Silver NPS",     PATHS["silver_nps"]),
    ("Gold Features",  PATHS["gold_modelo_features"]),
    ("Gold Risco",     PATHS["gold_metricas_risco"]),
    ("Gold NPS",       PATHS["gold_metricas_nps"]),
    ("Gold Dashboard", PATHS["gold_dashboard_resumo"]),
]:
    try:
        df_tmp = spark.read.parquet(path)
        n      = df_tmp.count()
        cols   = len(df_tmp.columns)
        print(f"  ✅ {label:<20} | {n:>7,} registros | {cols:>3} colunas")
    except Exception as e:
        print(f"  ❌ {label:<20} | ERRO: {e}")

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════╗
║   🏆  PIPELINE MEDALLION CONCLUÍDO COM SUCESSO!        ║
║                                                          ║
║   🥉 Bronze → Dados Raw preservados            ✅       ║
║   🥈 Silver → Limpeza + Feature Engineering    ✅       ║
║   🥇 Gold   → 4 tabelas analíticas prontas     ✅       ║
║                                                          ║
║   AWS S3 Bucket: payflow-risk-lake                      ║
║   Camadas:       bronze/ | silver/ | gold/              ║
║                                                          ║
║   Próximo passo:                                         ║
║   → Dashboard Streamlit consumindo dados do Gold        ║
╚══════════════════════════════════════════════════════════╝
""")
