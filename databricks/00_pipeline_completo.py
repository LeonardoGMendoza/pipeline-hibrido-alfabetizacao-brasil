# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Alfabetização Brasil — Pipeline Medallion
# MAGIC ## Databricks Community + AWS Academy Learner Lab
# MAGIC 
# MAGIC **Grupo FIAP AI Scientist:**
# MAGIC - Leonardo Jr. G. Mendoza (RM 373713)
# MAGIC - Caio Morais Rubino (RM 371492)
# MAGIC - Winny Tavares (RM 371471)
# MAGIC - Reinaldo Fernandes (RM 371717)
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 📋 Como usar este notebook:
# MAGIC 1. Execute as células na ordem
# MAGIC 2. Cole as credenciais da AWS na célula de configuração
# MAGIC 3. O notebook gerará toda a arquitetura Bronze → Silver → Gold no S3.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ CÉLULA 1 — Dependências

# COMMAND ----------

# MAGIC %pip install boto3 pandas numpy pyarrow

# COMMAND ----------

# MAGIC %md
# MAGIC ## ☁️ CÉLULA 2 — Configuração AWS S3
# MAGIC ### ⚠️ SUBSTITUA AS CREDENCIAIS ABAIXO!
# MAGIC ### (AWS Academy → Start Lab → AWS Details → Show)

# COMMAND ----------

import os

# ====================================================
# ⬇️ COLE AQUI AS CREDENCIAIS DA AWS ACADEMY
# ====================================================
AWS_ACCESS_KEY_ID     = "COLE_AQUI"
AWS_SECRET_ACCESS_KEY = "COLE_AQUI"
AWS_SESSION_TOKEN     = "COLE_AQUI"
AWS_REGION            = "us-east-1"
S3_BUCKET             = "alfabetizacao-datalake"

os.environ["AWS_ACCESS_KEY_ID"]     = AWS_ACCESS_KEY_ID
os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
os.environ["AWS_SESSION_TOKEN"]     = AWS_SESSION_TOKEN

print("✅ Credenciais configuradas!")
print(f"🪣 Bucket S3: s3://{S3_BUCKET}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏗️ CÉLULA 3 — Criação do Bucket S3

# COMMAND ----------

import boto3
from botocore.exceptions import ClientError
import io
import pandas as pd
import numpy as np
from datetime import datetime

s3 = boto3.client("s3", region_name=AWS_REGION)

try:
    s3.create_bucket(Bucket=S3_BUCKET)
    print(f"✅ Bucket s3://{S3_BUCKET}/ criado!")
except ClientError as e:
    if e.response["Error"]["Code"] in ["BucketAlreadyOwnedByYou", "BucketAlreadyExists"]:
        print(f"ℹ️  Bucket já existe.")
    else:
        raise e

# Pastas Medallion
for pasta in ["bronze/", "silver/", "gold/"]:
    s3.put_object(Bucket=S3_BUCKET, Key=f"{pasta}.keep", Body=b"")
    print(f"  📁 s3://{S3_BUCKET}/{pasta}")

def df_para_s3_parquet(df, bucket, key):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    print(f"  💾 Salvo: s3://{bucket}/{key} ({len(df):,} linhas)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥉 CÉLULA 4 — BRONZE: Ingestão Raw
# MAGIC *Simulando a ingestão dos Microdados do INEP*

# COMMAND ----------

print("=" * 55)
print("🥉 CAMADA BRONZE — Dados Brutos (INEP)")
print("=" * 55)

np.random.seed(42)
n_alunos = 10000

# Simulando Municípios
df_mun_bronze = pd.DataFrame({
    "CO_MUNICIPIO": [3550308, 3304557, 3106200, 4106902, 4314902],
    "NO_MUNICIPIO": ["São Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba", "Porto Alegre"],
    "SG_UF": ["SP", "RJ", "MG", "PR", "RS"],
    "_bronze_ts": datetime.now().isoformat()
})

# Simulando Alunos
codigos_mun = df_mun_bronze["CO_MUNICIPIO"].tolist()
df_aluno_bronze = pd.DataFrame({
    "ID_ALUNO": range(1, n_alunos + 1),
    "CO_MUNICIPIO": np.random.choice(codigos_mun, n_alunos),
    "IN_PRESENCA_LP": np.random.choice([0, 1], n_alunos, p=[0.05, 0.95]),  # 95% presentes
    "VL_PROFICIENCIA_LP": np.random.normal(740, 50, n_alunos), # Média 740, std 50
    "_bronze_ts": datetime.now().isoformat()
})

# Inserindo alguns nulos para simular sujeira do INEP
df_aluno_bronze.loc[df_aluno_bronze["IN_PRESENCA_LP"] == 0, "VL_PROFICIENCIA_LP"] = np.nan

df_para_s3_parquet(df_mun_bronze, S3_BUCKET, "bronze/municipios/dados.parquet")
df_para_s3_parquet(df_aluno_bronze, S3_BUCKET, "bronze/alunos/dados.parquet")

print("✅ BRONZE CONCLUÍDO!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥈 CÉLULA 5 — SILVER: Limpeza e JOIN

# COMMAND ----------

print("=" * 55)
print("🥈 CAMADA SILVER — Limpeza + Qualidade")
print("=" * 55)

# 1. Filtro de Presença e Remoção de Metadados
df_aluno_silver = df_aluno_bronze[df_aluno_bronze["IN_PRESENCA_LP"] == 1].copy()
df_aluno_silver = df_aluno_silver.drop(columns=["IN_PRESENCA_LP", "_bronze_ts"])
df_mun_silver = df_mun_bronze.drop(columns=["_bronze_ts"])

# 2. Agregação Inicial (redução drástica de volume)
df_aluno_agg = df_aluno_silver.groupby("CO_MUNICIPIO").agg(
    proficiencia_media_alunos = ("VL_PROFICIENCIA_LP", "mean"),
    qtd_alunos_avaliados = ("ID_ALUNO", "count")
).reset_index()

# 3. JOIN Município + Alunos
df_silver_integrado = pd.merge(df_mun_silver, df_aluno_agg, on="CO_MUNICIPIO", how="left")

print(f"✅ Agregação reduzida para {len(df_silver_integrado)} municípios.")

df_para_s3_parquet(df_silver_integrado, S3_BUCKET, "silver/dados_integrados/dados.parquet")
print("✅ SILVER CONCLUÍDO!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥇 CÉLULA 6 — GOLD: Regras de Negócio e Tabela Analítica

# COMMAND ----------

print("=" * 55)
print("🥇 CAMADA GOLD — Indicadores de Negócio")
print("=" * 55)

# Aplicação da regra do SAEB: >= 743 pts = Alfabetizado
def definir_status(nota):
    if pd.isna(nota): return "Sem Dados"
    return "Meta Atingida (≥ 743 pts)" if nota >= 743.0 else "Atenção (< 743 pts)"

df_gold_alfabetizacao = pd.DataFrame({
    'id_municipio': df_silver_integrado['CO_MUNICIPIO'],
    'nome_municipio': df_silver_integrado['NO_MUNICIPIO'],
    'sigla_uf': df_silver_integrado['SG_UF'],
    'qtd_alunos_avaliados': df_silver_integrado['qtd_alunos_avaliados'].fillna(0).astype(int),
    'proficiencia_media': df_silver_integrado['proficiencia_media_alunos'].round(2),
    'status_alfabetizacao': df_silver_integrado['proficiencia_media_alunos'].apply(definir_status)
})

print(df_gold_alfabetizacao.to_string(index=False))

df_para_s3_parquet(df_gold_alfabetizacao, S3_BUCKET, "gold/indicador_alfabetizacao/dados.parquet")
print("\n✅ GOLD CONCLUÍDO!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 CÉLULA 7 — Validação e Output no S3

# COMMAND ----------

print(f"📋 Conteúdo completo do bucket s3://{S3_BUCKET}/:\n")
paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=S3_BUCKET):
    for obj in page.get("Contents", []):
        tamanho_kb = obj["Size"] / 1024
        print(f"  ✅ s3://{S3_BUCKET}/{obj['Key']} ({tamanho_kb:.1f} KB)")
        
print("""
╔═══════════════════════════════════════════════════════╗
║   🏆  PIPELINE INEP MEDALLION EXECUTADO!              ║
║   Dados prontos para o MongoDB e Dashboard Streamlit  ║
╚═══════════════════════════════════════════════════════╝
""")
