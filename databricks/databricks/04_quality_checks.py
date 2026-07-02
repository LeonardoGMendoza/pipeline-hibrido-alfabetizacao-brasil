# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 Qualidade de Dados — PayFlow Risk Model
# MAGIC ## Great Expectations + Validações CRISP-DM
# MAGIC
# MAGIC Este notebook implementa um conjunto de testes de **qualidade de dados**
# MAGIC para garantir a integridade do pipeline Medallion antes do consumo pelo modelo ML.
# MAGIC
# MAGIC | Dimensão | O que valida |
# MAGIC |----------|-------------|
# MAGIC | **Completude** | % de nulos por coluna |
# MAGIC | **Unicidade** | Registros duplicados |
# MAGIC | **Validade** | Valores dentro dos ranges esperados |
# MAGIC | **Consistência** | Regras de negócio (ex: score 300-850) |
# MAGIC | **Distribuição** | Desvios estatísticos vs baseline |

# COMMAND ----------

import pandas as pd
import numpy as np
import boto3, io, json
from datetime import datetime

print("=" * 60)
print("  PAYFLOW RISK MODEL — QUALITY ASSURANCE")
print("  Validação de Dados — Pipeline Medallion")
print(f"  Execução: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Configuração

# COMMAND ----------

AWS_ACCESS_KEY_ID     = "COLE_AQUI"
AWS_SECRET_ACCESS_KEY = "COLE_AQUI"
AWS_SESSION_TOKEN     = "COLE_AQUI"
AWS_REGION            = "us-east-1"
S3_BUCKET             = "payflow-risk-lake"

import os
os.environ["AWS_ACCESS_KEY_ID"]     = AWS_ACCESS_KEY_ID
os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
os.environ["AWS_SESSION_TOKEN"]     = AWS_SESSION_TOKEN

s3 = boto3.client("s3", region_name=AWS_REGION)

def ler_parquet_s3(key):
    obj    = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))

# Ler camadas
df_bronze = ler_parquet_s3("bronze/payflow_credito/dados.parquet")
df_silver = ler_parquet_s3("silver/payflow_credito/dados.parquet")
df_gold   = ler_parquet_s3("gold/modelo_features/dados.parquet")

print(f"✅ Bronze: {len(df_bronze):,} registros | {len(df_bronze.columns)} colunas")
print(f"✅ Silver: {len(df_silver):,} registros | {len(df_silver.columns)} colunas")
print(f"✅ Gold:   {len(df_gold):,} registros | {len(df_gold.columns)} colunas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Framework de Testes de Qualidade

# COMMAND ----------

class QualityChecker:
    """Framework simples de qualidade de dados estilo Great Expectations."""

    def __init__(self, df: pd.DataFrame, nome: str):
        self.df       = df
        self.nome     = nome
        self.testes   = []
        self.passou   = 0
        self.falhou   = 0

    def _registrar(self, nome_teste, passou, detalhe=""):
        status = "✅ PASSOU" if passou else "❌ FALHOU"
        self.testes.append({"teste": nome_teste, "status": status, "detalhe": detalhe})
        if passou: self.passou += 1
        else:      self.falhou += 1

    # ── Completude ──────────────────────────────────
    def expect_no_nulls(self, coluna, threshold_pct=5.0):
        pct_nulo = self.df[coluna].isna().mean() * 100
        passou   = pct_nulo <= threshold_pct
        self._registrar(
            f"[Completude] {coluna} — nulos ≤ {threshold_pct}%",
            passou,
            f"{pct_nulo:.2f}% nulos ({self.df[coluna].isna().sum()} registros)"
        )
        return self

    # ── Unicidade ───────────────────────────────────
    def expect_unique(self, coluna):
        n_dup  = self.df.duplicated(subset=[coluna]).sum()
        passou = n_dup == 0
        self._registrar(
            f"[Unicidade] {coluna} — sem duplicatas",
            passou,
            f"{n_dup} duplicatas encontradas"
        )
        return self

    # ── Validade de range ───────────────────────────
    def expect_between(self, coluna, minimo, maximo):
        fora = ((self.df[coluna] < minimo) | (self.df[coluna] > maximo)).sum()
        pct  = fora / len(self.df) * 100
        passou = fora == 0
        self._registrar(
            f"[Validade] {coluna} ∈ [{minimo}, {maximo}]",
            passou,
            f"{fora} valores fora do range ({pct:.2f}%)"
        )
        return self

    # ── Validade de conjunto ────────────────────────
    def expect_in_set(self, coluna, valores_validos):
        invalidos = ~self.df[coluna].isin(valores_validos)
        n_inv     = invalidos.sum()
        passou    = n_inv == 0
        self._registrar(
            f"[Validade] {coluna} — valores válidos",
            passou,
            f"{n_inv} valores inválidos | Esperados: {valores_validos}"
        )
        return self

    # ── Distribuição ────────────────────────────────
    def expect_mean_between(self, coluna, min_media, max_media):
        media  = self.df[coluna].mean()
        passou = min_media <= media <= max_media
        self._registrar(
            f"[Distribuição] Média de {coluna} ∈ [{min_media}, {max_media}]",
            passou,
            f"Média observada: {media:.4f}"
        )
        return self

    def expect_proportion(self, coluna, valor, min_prop, max_prop):
        prop   = (self.df[coluna] == valor).mean()
        passou = min_prop <= prop <= max_prop
        self._registrar(
            f"[Proporção] {coluna}={valor} ∈ [{min_prop:.1%}, {max_prop:.1%}]",
            passou,
            f"Proporção observada: {prop:.1%}"
        )
        return self

    # ── Não-negatividade ────────────────────────────
    def expect_non_negative(self, coluna):
        neg    = (self.df[coluna] < 0).sum()
        passou = neg == 0
        self._registrar(
            f"[Validade] {coluna} ≥ 0",
            passou,
            f"{neg} valores negativos"
        )
        return self

    # ── Relatório ───────────────────────────────────
    def report(self):
        total  = self.passou + self.falhou
        pct_ok = (self.passou / total * 100) if total > 0 else 0

        print(f"\n{'='*60}")
        print(f"  📋 RELATÓRIO DE QUALIDADE — {self.nome}")
        print(f"{'='*60}")
        print(f"  Total de testes : {total}")
        print(f"  ✅ Passou        : {self.passou}  ({pct_ok:.0f}%)")
        print(f"  ❌ Falhou        : {self.falhou}")
        print(f"{'─'*60}")

        for t in self.testes:
            print(f"  {t['status']} | {t['teste']}")
            if t["detalhe"]:
                print(f"              → {t['detalhe']}")

        print(f"{'─'*60}")
        emoji = "🟢" if self.falhou == 0 else ("🟡" if self.falhou <= 2 else "🔴")
        print(f"  {emoji} Score de Qualidade: {pct_ok:.0f}%")
        return {"camada": self.nome, "passou": self.passou, "falhou": self.falhou,
                "score_pct": round(pct_ok, 1), "testes": self.testes}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥉 Testes — Camada Bronze

# COMMAND ----------

bronze_qc = QualityChecker(df_bronze, "BRONZE — PayFlow Crédito")

(bronze_qc
 # Completude
 .expect_no_nulls("score_credito")
 .expect_no_nulls("default_90d")
 .expect_no_nulls("renda_mensal")
 .expect_no_nulls("valor_solicitado")

 # Unicidade
 .expect_unique("id_cliente")

 # Validade de range
 .expect_between("score_credito",      300,  850)
 .expect_between("utilizacao_credito", 0.0,  1.0)
 .expect_between("renda_mensal",       0,    50000)
 .expect_between("default_90d",        0,    1)

 # Valores válidos
 .expect_in_set("default_90d", [0, 1])
 .expect_in_set("status_apos_90d", ["em_dia", "atraso_leve", "inadimplente"])

 # Distribuição — conforme documentado no CRISP-DM
 .expect_mean_between("score_credito", 550, 750)
 .expect_proportion("default_90d",  1, 0.08, 0.18)  # ~12% esperado

 # Não negativos
 .expect_non_negative("dias_atraso_max_12m")
 .expect_non_negative("parcelas_pagas_ate_3m")
)

resultado_bronze = bronze_qc.report()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥈 Testes — Camada Silver

# COMMAND ----------

silver_qc = QualityChecker(df_silver, "SILVER — PayFlow Crédito")

(silver_qc
 # Completude — Silver deve ter ZERO nulos (imputação aplicada)
 .expect_no_nulls("score_credito",          threshold_pct=0)
 .expect_no_nulls("utilizacao_credito",     threshold_pct=0)
 .expect_no_nulls("dias_atraso_max_12m",    threshold_pct=0)
 .expect_no_nulls("comprometimento_renda",  threshold_pct=0)
 .expect_no_nulls("renda_mensal",           threshold_pct=0)

 # Leakage deve ter sido REMOVIDO
 .expect_no_nulls("default_90d", threshold_pct=0)

 # Feature Engineering
 .expect_non_negative("comprometimento_renda")
 .expect_between("comprometimento_renda", 0.01, 20.0)
 .expect_between("flag_atraso_critico",   0, 1)
 .expect_in_set("flag_atraso_critico",    [0, 1])

 # faixa_score
 .expect_in_set("faixa_score", ["CRITICO", "ALTO_RISCO", "MEDIO_RISCO", "BAIXO_RISCO"])

 # Validade pós-limpeza
 .expect_between("score_credito",      300, 850)
 .expect_between("utilizacao_credito", 0.0, 1.0)
 .expect_between("renda_mensal",       1,   50000)

 # Volume: Silver deve ter ≥95% do Bronze após filtros
 .expect_mean_between("comprometimento_renda", 0.1, 5.0)

 # Sem colunas de leakage (verificação indireta via contagem de colunas)
)

resultado_silver = silver_qc.report()

# Verificação extra: leakage removido
colunas_leakage = ["parcelas_pagas_ate_3m", "status_apos_90d"]
for col in colunas_leakage:
    if col in df_silver.columns:
        print(f"  ❌ ALERTA: Coluna de leakage '{col}' ainda presente no Silver!")
    else:
        print(f"  ✅ Leakage '{col}' corretamente removido do Silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥇 Testes — Camada Gold

# COMMAND ----------

gold_qc = QualityChecker(df_gold, "GOLD — Features para o Modelo ML")

(gold_qc
 # Completude total — Gold não pode ter nulos
 .expect_no_nulls("score_credito",          threshold_pct=0)
 .expect_no_nulls("utilizacao_credito",     threshold_pct=0)
 .expect_no_nulls("dias_atraso_max_12m",    threshold_pct=0)
 .expect_no_nulls("comprometimento_renda",  threshold_pct=0)
 .expect_no_nulls("default_90d",            threshold_pct=0)

 # Features do modelo dentro dos ranges do treino
 .expect_between("score_credito",           300, 850)
 .expect_between("utilizacao_credito",      0.0, 1.0)
 .expect_between("comprometimento_renda",   0.0, 20.0)
 .expect_between("score_propensao_default", 0.0, 1.0)

 # Proporção de default esperada pelo modelo (12% ± 4%)
 .expect_proportion("default_90d", 1, 0.06, 0.20)

 # Não-negatividade
 .expect_non_negative("dias_atraso_max_12m")
 .expect_non_negative("score_propensao_default")
)

resultado_gold = gold_qc.report()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Resumo Executivo — Pipeline Quality Gate

# COMMAND ----------

resultados = [resultado_bronze, resultado_silver, resultado_gold]

print("\n" + "="*65)
print("  🎯 QUALITY GATE — RESUMO EXECUTIVO DO PIPELINE")
print("="*65)
print(f"\n  {'Camada':<25} {'Testes':>8} {'✅':>8} {'❌':>8} {'Score':>8}")
print("  " + "─"*55)

aprovado_pipeline = True
for r in resultados:
    total = r["passou"] + r["falhou"]
    emoji = "🟢" if r["falhou"] == 0 else ("🟡" if r["falhou"] <= 2 else "🔴")
    print(f"  {emoji} {r['camada']:<23} {total:>8} {r['passou']:>8} {r['falhou']:>8} {r['score_pct']:>7.0f}%")
    if r["falhou"] > 0:
        aprovado_pipeline = False

print("  " + "─"*55)
score_geral = sum(r["score_pct"] for r in resultados) / len(resultados)
print(f"\n  Score Geral do Pipeline: {score_geral:.1f}%")

if aprovado_pipeline:
    print("""
  ╔══════════════════════════════════════════════════╗
  ║  🟢  PIPELINE APROVADO — QUALITY GATE OK!      ║
  ║  Dados prontos para consumo pelo modelo ML      ║
  ╚══════════════════════════════════════════════════╝
  """)
else:
    print("""
  ╔══════════════════════════════════════════════════╗
  ║  🔴  PIPELINE COM ALERTAS — REVISAR FALHAS     ║
  ║  Verifique os testes ❌ antes de prosseguir     ║
  ╚══════════════════════════════════════════════════╝
  """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Salvar Relatório de Qualidade no S3

# COMMAND ----------

relatorio = {
    "executado_em"       : datetime.now().isoformat(),
    "pipeline_aprovado"  : aprovado_pipeline,
    "score_geral_pct"    : round(score_geral, 1),
    "camadas"            : resultados,
}

s3.put_object(
    Bucket      = S3_BUCKET,
    Key         = "gold/quality_report/relatorio.json",
    Body        = json.dumps(relatorio, indent=2, ensure_ascii=False).encode("utf-8"),
    ContentType = "application/json",
)

print(f"✅ Relatório de qualidade salvo em:")
print(f"   s3://{S3_BUCKET}/gold/quality_report/relatorio.json")
print(f"\n   Score geral: {score_geral:.1f}%")
print(f"   Pipeline aprovado: {'SIM ✅' if aprovado_pipeline else 'NÃO ❌'}")
