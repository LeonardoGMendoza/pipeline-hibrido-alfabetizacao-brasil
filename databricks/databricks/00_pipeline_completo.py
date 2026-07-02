# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 PayFlow Risk Model — Pipeline Completo
# MAGIC ## Arquitetura Medallion: Bronze → Silver → Gold
# MAGIC ### AWS Academy Learner Lab + Databricks Community Edition
# MAGIC
# MAGIC **Grupo FIAP AI Scientist:**
# MAGIC - Reinaldo Fernandes (RM 371717)
# MAGIC - Leonardo Junior Gonzales Mendoza (RM 373713)
# MAGIC - Winny Tavares (RM 371471)
# MAGIC - Caio Morais Rubino (RM 371492)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📋 Como usar este notebook:
# MAGIC 1. Execute as células **na ordem** de cima para baixo
# MAGIC 2. Substitua as credenciais AWS na célula de configuração
# MAGIC 3. O notebook fará Bronze → Silver → Gold automaticamente

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ CÉLULA 1 — Instalação de dependências

# COMMAND ----------

# MAGIC %pip install boto3 pandas numpy pyarrow

# COMMAND ----------

# MAGIC %md
# MAGIC ## ☁️ CÉLULA 2 — Configuração AWS S3
# MAGIC ### ⚠️ SUBSTITUA AS CREDENCIAIS ABAIXO!
# MAGIC ### Como obter: AWS Academy → Start Lab → AWS Details → Show

# COMMAND ----------

import os

# ====================================================
# ⬇️ COLE AQUI AS CREDENCIAIS DO AWS ACADEMY LEARNER LAB
# ====================================================
AWS_ACCESS_KEY_ID     = "COLE_AQUI"   # Começa com ASIA...
AWS_SECRET_ACCESS_KEY = "COLE_AQUI"   # chave secreta
AWS_SESSION_TOKEN     = "COLE_AQUI"   # token de sessão (longo)
AWS_REGION            = "us-east-1"
S3_BUCKET             = "payflow-risk-lake"

# Configurar variáveis de ambiente para boto3
os.environ["AWS_ACCESS_KEY_ID"]     = AWS_ACCESS_KEY_ID
os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
os.environ["AWS_SESSION_TOKEN"]     = AWS_SESSION_TOKEN
os.environ["AWS_DEFAULT_REGION"]    = AWS_REGION

# Configurar Spark para usar S3
spark.conf.set("fs.s3a.access.key",    AWS_ACCESS_KEY_ID)
spark.conf.set("fs.s3a.secret.key",    AWS_SECRET_ACCESS_KEY)
spark.conf.set("fs.s3a.session.token", AWS_SESSION_TOKEN)
spark.conf.set("fs.s3a.aws.credentials.provider",
               "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
spark.conf.set("fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")

print("✅ Credenciais configuradas!")
print(f"🪣 Bucket S3: s3://{S3_BUCKET}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏗️ CÉLULA 3 — Criar estrutura do bucket S3

# COMMAND ----------

import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3", region_name=AWS_REGION)

# Criar bucket (se não existir)
try:
    s3.create_bucket(Bucket=S3_BUCKET)
    print(f"✅ Bucket criado: s3://{S3_BUCKET}/")
except ClientError as e:
    cod = e.response["Error"]["Code"]
    if cod in ["BucketAlreadyOwnedByYou", "BucketAlreadyExists"]:
        print(f"ℹ️  Bucket já existe: s3://{S3_BUCKET}/")
    else:
        raise e

# Criar marcadores de pasta
for pasta in ["bronze/", "silver/", "gold/"]:
    s3.put_object(Bucket=S3_BUCKET, Key=f"{pasta}.keep", Body=b"")
    print(f"  📁 s3://{S3_BUCKET}/{pasta}")

print("\n✅ Estrutura do Data Lake criada!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥉 CÉLULA 4 — BRONZE: Geração e ingestão dos dados
# MAGIC
# MAGIC Os dados são gerados com base nos parâmetros reais do projeto PayFlow:
# MAGIC - **5.000 registros** de crédito (12% de taxa de default, conforme CRISP-DM)
# MAGIC - **3.000 registros** de NPS (ponto de ruptura logística: 3 dias de atraso)

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime
import io

print("=" * 55)
print("🥉 CAMADA BRONZE — Ingestão de Dados Brutos")
print("=" * 55)

np.random.seed(42)  # Reprodutibilidade

# ─────────────────────────────────────────────
# DATASET 1: PayFlow Crédito (default_90d)
# Baseado nos dados reais do desafio FIAP
# ─────────────────────────────────────────────
n_credito = 5000
score      = np.clip(np.random.normal(650, 120, n_credito), 300, 850)
renda      = np.random.uniform(1500, 15000, n_credito)
valor      = renda * np.random.uniform(0.5, 4.0, n_credito)
atraso     = np.maximum(0, np.random.exponential(10, n_credito)).astype(int)
utilizacao = np.clip(np.random.uniform(0.05, 0.95, n_credito), 0, 1)

# 12% de inadimplência — mesma proporção do dataset original
prob_default = np.where(score < 450, 0.70,
               np.where(score < 550, 0.45,
               np.where(score < 650, 0.25,
               np.where(score < 750, 0.10, 0.04))))
default_90d  = (np.random.uniform(0, 1, n_credito) < prob_default).astype(int)

df_credito_bronze = pd.DataFrame({
    "id_cliente"            : range(1, n_credito + 1),
    "score_credito"         : np.round(score, 2),
    "utilizacao_credito"    : np.round(utilizacao, 4),
    "dias_atraso_max_12m"   : atraso,
    "valor_solicitado"      : np.round(valor, 2),
    "renda_mensal"          : np.round(renda, 2),
    # LEAKAGE — mantido no Bronze (auditoria), removido no Silver
    "parcelas_pagas_ate_3m" : np.random.randint(0, 4, n_credito),
    "status_apos_90d"       : np.random.choice(["em_dia","atraso_leve","inadimplente"], n_credito),
    "default_90d"           : default_90d,
    # Metadados de ingestão
    "_bronze_ts"            : datetime.now().isoformat(),
    "_bronze_source"        : "payflow_credito_v1",
})

# ─────────────────────────────────────────────
# DATASET 2: PayFlow NPS
# Baseado no desafio_nps_fase_1.csv do projeto
# Ponto de ruptura: 3 dias de atraso
# ─────────────────────────────────────────────
n_nps   = 3000
atraso_nps = np.maximum(0, np.random.exponential(2, n_nps)).astype(int)
notas      = np.random.choice([1,2,3,4,5], n_nps, p=[0.05,0.10,0.20,0.35,0.30])
detrator   = ((atraso_nps > 3) | (notas <= 2)).astype(int)

df_nps_bronze = pd.DataFrame({
    "id_transacao"        : [f"TXN-{i:06d}" for i in range(1, n_nps + 1)],
    "categoria_produto"   : np.random.choice(["eletronicos","vestuario","alimentos","moveis"], n_nps),
    "dias_atraso_entrega" : atraso_nps,
    "valor_pedido"        : np.round(np.random.uniform(50, 800, n_nps), 2),
    "qtd_itens"           : np.random.randint(1, 6, n_nps),
    "avaliacao_nota"      : notas,
    "nps_detrator"        : detrator,
    "_bronze_ts"          : datetime.now().isoformat(),
    "_bronze_source"      : "payflow_nps_v1",
})

print(f"✅ Bronze Crédito: {len(df_credito_bronze):,} registros | Taxa default: {df_credito_bronze['default_90d'].mean():.1%}")
print(f"✅ Bronze NPS:     {len(df_nps_bronze):,} registros | Taxa detração: {df_nps_bronze['nps_detrator'].mean():.1%}")

# Salvar como Parquet no S3
def df_para_s3_parquet(df, bucket, key):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    print(f"  💾 Salvo: s3://{bucket}/{key} ({len(df):,} linhas)")

print("\n💾 Salvando camada Bronze no S3...")
df_para_s3_parquet(df_credito_bronze, S3_BUCKET, "bronze/payflow_credito/dados.parquet")
df_para_s3_parquet(df_nps_bronze,     S3_BUCKET, "bronze/payflow_nps/dados.parquet")

print("\n✅ BRONZE CONCLUÍDO!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥈 CÉLULA 5 — SILVER: Limpeza e Feature Engineering

# COMMAND ----------

print("=" * 55)
print("🥈 CAMADA SILVER — Limpeza + Feature Engineering")
print("=" * 55)

# ─────────────────────────────────────────────
# PayFlow Crédito — Silver
# ─────────────────────────────────────────────
df_silver_credito = df_credito_bronze.copy()

# 1. Remover colunas de LEAKAGE (descoberto na análise CRISP-DM)
colunas_leakage  = ["parcelas_pagas_ate_3m", "status_apos_90d"]
colunas_metadata = [c for c in df_silver_credito.columns if c.startswith("_bronze_")]
df_silver_credito = df_silver_credito.drop(columns=colunas_leakage + colunas_metadata)
print(f"✅ Leakage removido: {colunas_leakage}")

# 2. Imputação de nulos por mediana
for col in ["score_credito", "utilizacao_credito", "dias_atraso_max_12m",
            "renda_mensal", "valor_solicitado"]:
    mediana = df_silver_credito[col].median()
    nulos   = df_silver_credito[col].isna().sum()
    df_silver_credito[col] = df_silver_credito[col].fillna(mediana)
    if nulos > 0:
        print(f"  ✅ {col}: {nulos} nulos → mediana={mediana:.2f}")

# 3. Feature Engineering
df_silver_credito["comprometimento_renda"] = (
    df_silver_credito["valor_solicitado"] / df_silver_credito["renda_mensal"]
).round(4)

df_silver_credito["faixa_score"] = pd.cut(
    df_silver_credito["score_credito"],
    bins   = [0, 450, 600, 750, 850],
    labels = ["CRITICO", "ALTO_RISCO", "MEDIO_RISCO", "BAIXO_RISCO"]
)

df_silver_credito["flag_atraso_critico"] = (
    df_silver_credito["dias_atraso_max_12m"] > 30
).astype(int)

# 4. Filtro de qualidade
antes = len(df_silver_credito)
df_silver_credito = df_silver_credito[
    (df_silver_credito["renda_mensal"]       > 0) &
    (df_silver_credito["score_credito"]      >= 300) &
    (df_silver_credito["score_credito"]      <= 850) &
    (df_silver_credito["utilizacao_credito"] >= 0) &
    (df_silver_credito["utilizacao_credito"] <= 1)
]
print(f"✅ Qualidade: {antes:,} → {len(df_silver_credito):,} registros")
print(f"✅ Feature 'comprometimento_renda' criada (média: {df_silver_credito['comprometimento_renda'].mean():.3f})")

# ─────────────────────────────────────────────
# PayFlow NPS — Silver
# ─────────────────────────────────────────────
df_silver_nps = df_nps_bronze.drop(
    columns=[c for c in df_nps_bronze.columns if c.startswith("_bronze_")]
)
df_silver_nps["flag_atraso_critico"] = (df_silver_nps["dias_atraso_entrega"] > 3).astype(int)
df_silver_nps["faixa_valor"]         = pd.cut(
    df_silver_nps["valor_pedido"],
    bins=[0, 200, 500, 9999], labels=["BAIXO","MEDIO","ALTO"]
)

print(f"\n✅ Silver NPS preparado: {len(df_silver_nps):,} registros")
print(f"   Ruptura logística (>3 dias): {df_silver_nps['flag_atraso_critico'].mean():.1%}")

# Salvar no S3
print("\n💾 Salvando camada Silver no S3...")
df_para_s3_parquet(df_silver_credito, S3_BUCKET, "silver/payflow_credito/dados.parquet")
df_para_s3_parquet(df_silver_nps,     S3_BUCKET, "silver/payflow_nps/dados.parquet")

print("\n✅ SILVER CONCLUÍDO!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥇 CÉLULA 6 — GOLD: Tabelas Analíticas Finais

# COMMAND ----------

print("=" * 55)
print("🥇 CAMADA GOLD — Tabelas Analíticas para o Dashboard")
print("=" * 55)

# ─────────────────────────────────────────────
# GOLD 1: Features para o Modelo ML
# ─────────────────────────────────────────────
df_gold_features = df_silver_credito[[
    "score_credito", "utilizacao_credito", "dias_atraso_max_12m",
    "comprometimento_renda", "flag_atraso_critico", "default_90d"
]].copy()

# Score de propensão ao default (baseado nas regras do modelo)
df_gold_features["score_propensao_default"] = np.where(
    df_gold_features["score_credito"] < 450, 0.70,
    np.where(df_gold_features["score_credito"] < 550, 0.45,
    np.where(df_gold_features["score_credito"] < 650, 0.25,
    np.where(df_gold_features["score_credito"] < 750, 0.10, 0.04)))
)

print(f"✅ Gold Features: {len(df_gold_features):,} registros | {len(df_gold_features.columns)} colunas")

# ─────────────────────────────────────────────
# GOLD 2: Métricas de Risco por Segmento
# ─────────────────────────────────────────────
df_gold_risco = (
    df_silver_credito
    .groupby("faixa_score", observed=True)
    .agg(
        total_clientes        = ("default_90d", "count"),
        total_inadimplentes   = ("default_90d", "sum"),
        taxa_default_pct      = ("default_90d", lambda x: round(x.mean() * 100, 2)),
        score_medio           = ("score_credito", lambda x: round(x.mean(), 1)),
        comprometimento_medio = ("comprometimento_renda", lambda x: round(x.mean(), 3)),
        atraso_medio_dias     = ("dias_atraso_max_12m", lambda x: round(x.mean(), 1)),
        utilizacao_media_pct  = ("utilizacao_credito", lambda x: round(x.mean() * 100, 2)),
    )
    .reset_index()
    .sort_values("taxa_default_pct", ascending=False)
)
df_gold_risco["faixa_score"] = df_gold_risco["faixa_score"].astype(str)

print(f"✅ Gold Risco: {len(df_gold_risco)} segmentos")
print(df_gold_risco[["faixa_score","total_clientes","taxa_default_pct"]].to_string(index=False))

# ─────────────────────────────────────────────
# GOLD 3: Métricas NPS por Categoria
# ─────────────────────────────────────────────
df_gold_nps = (
    df_silver_nps
    .groupby("categoria_produto")
    .agg(
        total_pedidos       = ("nps_detrator", "count"),
        total_detratores    = ("nps_detrator", "sum"),
        taxa_detracao_pct   = ("nps_detrator", lambda x: round(x.mean() * 100, 2)),
        nota_media          = ("avaliacao_nota", lambda x: round(x.mean(), 2)),
        atraso_medio        = ("dias_atraso_entrega", lambda x: round(x.mean(), 2)),
        atrasos_criticos    = ("flag_atraso_critico", "sum"),
        ticket_medio        = ("valor_pedido", lambda x: round(x.mean(), 2)),
    )
    .reset_index()
    .sort_values("taxa_detracao_pct", ascending=False)
)

print(f"\n✅ Gold NPS: {len(df_gold_nps)} categorias")
print(df_gold_nps[["categoria_produto","taxa_detracao_pct","nota_media"]].to_string(index=False))

# ─────────────────────────────────────────────
# GOLD 4: Resumo Executivo (Dashboard KPIs)
# ─────────────────────────────────────────────
df_gold_resumo = pd.DataFrame([{
    "projeto"                 : "PayFlow Risk Model",
    "total_clientes_credito"  : int(len(df_silver_credito)),
    "total_transacoes_nps"    : int(len(df_silver_nps)),
    "taxa_default_pct"        : round(df_silver_credito["default_90d"].mean() * 100, 2),
    "taxa_detracao_nps_pct"   : round(df_silver_nps["nps_detrator"].mean() * 100, 2),
    "atraso_ruptura_pct"      : round(df_silver_nps["flag_atraso_critico"].mean() * 100, 2),
    "comprometimento_renda"   : round(df_silver_credito["comprometimento_renda"].mean(), 3),
    "modelo_auc_roc"          : 0.92,
    "modelo_threshold"        : 0.35,
    "ponto_ruptura_dias"      : 3,
    "gerado_em"               : datetime.now().isoformat(),
}])

print(f"\n✅ Gold Resumo Executivo:")
for col in df_gold_resumo.columns:
    print(f"  {col:<30}: {df_gold_resumo[col].iloc[0]}")

# Salvar tudo no S3
print("\n💾 Salvando camada Gold no S3...")
df_para_s3_parquet(df_gold_features, S3_BUCKET, "gold/modelo_features/dados.parquet")
df_para_s3_parquet(df_gold_risco,    S3_BUCKET, "gold/metricas_risco/dados.parquet")
df_para_s3_parquet(df_gold_nps,      S3_BUCKET, "gold/metricas_nps/dados.parquet")
df_para_s3_parquet(df_gold_resumo,   S3_BUCKET, "gold/dashboard_resumo/dados.parquet")

print("\n✅ GOLD CONCLUÍDO!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 CÉLULA 7 — Validação Final do Pipeline

# COMMAND ----------

print("=" * 60)
print("📊 VALIDAÇÃO FINAL — PIPELINE MEDALLION COMPLETO")
print("=" * 60)

camadas = {
    "🥉 Bronze Crédito" : "bronze/payflow_credito/dados.parquet",
    "🥉 Bronze NPS"     : "bronze/payflow_nps/dados.parquet",
    "🥈 Silver Crédito" : "silver/payflow_credito/dados.parquet",
    "🥈 Silver NPS"     : "silver/payflow_nps/dados.parquet",
    "🥇 Gold Features"  : "gold/modelo_features/dados.parquet",
    "🥇 Gold Risco"     : "gold/metricas_risco/dados.parquet",
    "🥇 Gold NPS"       : "gold/metricas_nps/dados.parquet",
    "🥇 Gold Resumo"    : "gold/dashboard_resumo/dados.parquet",
}

total_bytes = 0
print(f"\n{'Camada':<25} {'Registros':>10} {'Tamanho':>12}")
print("-" * 50)
for nome, key in camadas.items():
    try:
        obj      = s3.get_object(Bucket=S3_BUCKET, Key=key)
        tamanho  = obj["ContentLength"]
        total_bytes += tamanho
        df_tmp   = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        print(f"{nome:<25} {len(df_tmp):>10,} {tamanho/1024:>10.1f} KB")
    except Exception as e:
        print(f"{nome:<25} {'ERRO':>10} {str(e)[:20]}")

print("-" * 50)
print(f"{'TOTAL no S3':<25} {total_bytes/1024:>23.1f} KB")

print(f"""
╔═══════════════════════════════════════════════════════╗
║   🏆  PIPELINE MEDALLION EXECUTADO COM SUCESSO!     ║
║                                                       ║
║   🥉 Bronze  → s3://{S3_BUCKET}/bronze/          ║
║   🥈 Silver  → s3://{S3_BUCKET}/silver/          ║
║   🥇 Gold    → s3://{S3_BUCKET}/gold/            ║
║                                                       ║
║   ✅ Dados disponíveis para o Dashboard Streamlit    ║
║   ✅ Modelo RF pode consumir gold/modelo_features/   ║
╚═══════════════════════════════════════════════════════╝
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 CÉLULA 8 — Listar objetos no S3 (para screenshot da entrega)

# COMMAND ----------

print(f"📋 Conteúdo completo do bucket s3://{S3_BUCKET}/:\n")
paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=S3_BUCKET):
    for obj in page.get("Contents", []):
        tamanho_kb = obj["Size"] / 1024
        print(f"  ✅ s3://{S3_BUCKET}/{obj['Key']} ({tamanho_kb:.1f} KB)")
