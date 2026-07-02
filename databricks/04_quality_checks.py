# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 Qualidade de Dados — Alfabetização INEP
# MAGIC ## Great Expectations + Data Quality Gate
# MAGIC 
# MAGIC Este notebook valida os dados transformados do INEP (Bronze/Silver/Gold).

# COMMAND ----------

import pandas as pd
import boto3, io, json
from datetime import datetime

print("=" * 60)
print("  ALFABETIZAÇÃO BRASIL — QUALITY ASSURANCE")
print(f"  Execução: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

AWS_ACCESS_KEY_ID     = "COLE_AQUI"
AWS_SECRET_ACCESS_KEY = "COLE_AQUI"
AWS_SESSION_TOKEN     = "COLE_AQUI"
AWS_REGION            = "us-east-1"
S3_BUCKET             = "alfabetizacao-datalake"

s3 = boto3.client(
    "s3", 
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN
)

def ler_parquet_s3(key):
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))

# Lendo o dado final (Gold)
df_gold = ler_parquet_s3("gold/indicador_alfabetizacao/dados.parquet")
print(f"✅ Camada Gold carregada: {len(df_gold)} registros")

# COMMAND ----------

testes, passou, falhou = [], 0, 0

def checar(nome, condicao, detalhe=""):
    global passou, falhou
    ok = bool(condicao)
    status = "✅ PASSOU" if ok else "❌ FALHOU"
    testes.append({"teste": nome, "status": status})
    if ok: passou += 1 
    else: falhou += 1
    print(f"  {status} | {nome}")
    if detalhe: print(f"             → {detalhe}")

print("\n🔍 INICIANDO VALIDAÇÕES GOLD:")

checar("Unicidade: id_municipio único por linha", 
       df_gold["id_municipio"].duplicated().sum() == 0,
       f"{df_gold['id_municipio'].duplicated().sum()} duplicatas")

checar("Completude: status_alfabetizacao sem nulos",
       df_gold["status_alfabetizacao"].isna().sum() == 0)

checar("Validade: qtd_alunos_avaliados >= 0",
       (df_gold["qtd_alunos_avaliados"] >= 0).all())

status_esperados = ["Meta Atingida (≥ 743 pts)", "Atenção (< 743 pts)", "Sem Dados"]
checar("Consistência: status dentro do padrão SAEB",
       df_gold["status_alfabetizacao"].isin(status_esperados).all())

print("\n" + "="*50)
score = (passou / (passou + falhou)) * 100
if falhou == 0:
    print(f"🟢 QUALITY GATE APROVADO! Score: {score:.0f}%")
else:
    print(f"🔴 QUALITY GATE FALHOU! Score: {score:.0f}%")
print("="*50)
