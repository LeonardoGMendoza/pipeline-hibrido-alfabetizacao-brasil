# 🏗️ Arquitetura Medallion — PayFlow Risk Model

## Visão Geral

Este documento descreve a **Arquitetura de Dados** implementada no projeto PayFlow Risk Model, seguindo o padrão **Medallion (Bronze → Silver → Gold)** no **Databricks**, com armazenamento no **AWS S3 (Academy Learner Lab)**.

---

## 🔷 Diagrama da Arquitetura

```
┌─────────────────────────────────────────────────────────────────────┐
│                    PAYFLOW RISK MODEL — DATA PIPELINE               │
│                    Arquitetura Medallion no AWS S3                  │
└─────────────────────────────────────────────────────────────────────┘

  ┌──────────────┐     ┌──────────────────────────────────────────┐
  │  FONTE RAW   │     │         DATABRICKS (Processamento)        │
  │              │     │                                           │
  │  CSV Crédito │────▶│  Notebook 01         Notebook 02         │
  │  CSV NPS     │     │  Bronze              Silver               │
  └──────────────┘     │  Ingestão            Limpeza +            │
                       │  Raw                 Feature Eng.         │
                       └────────────┬─────────────────────────────┘
                                    │
                                    ▼
  ┌───────────────────────────────────────────────────────────────┐
  │                   AWS S3 — payflow-risk-lake                  │
  │                                                               │
  │  s3://payflow-risk-lake/                                      │
  │  ├── bronze/                  ← Parquet Raw (sem transformar) │
  │  │   ├── payflow_credito/                                     │
  │  │   └── payflow_nps/                                         │
  │  ├── silver/                  ← Parquet Limpo + Features      │
  │  │   ├── payflow_credito/  (particionado por faixa_score)     │
  │  │   └── payflow_nps/      (particionado por categoria)       │
  │  └── gold/                   ← Tabelas Analíticas Finais      │
  │      ├── modelo_features/    ← Input direto para o modelo ML  │
  │      ├── metricas_risco/     ← KPIs por segmento de crédito   │
  │      ├── metricas_nps/       ← KPIs por categoria NPS         │
  │      └── dashboard_resumo/   ← Resumo executivo               │
  └───────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
  ┌───────────────────────────────────────────────────────────────┐
  │                   CONSUMO (GitHub → Dashboard)                │
  │                                                               │
  │  src/data_loader_aws.py  ← Conecta ao S3 via boto3           │
  │  src/app_tech_challenge.py ← Dashboard Streamlit             │
  │  models/modelo_risco_payflow.pkl ← Modelo RF treinado        │
  └───────────────────────────────────────────────────────────────┘
```

---

## 📒 Notebooks Databricks

| # | Arquivo | Camada | Responsabilidade |
|---|---------|--------|-----------------|
| 1 | `01_bronze_ingestao.py` | 🥉 Bronze | Ingestão raw do CSV, persistência Parquet no S3 |
| 2 | `02_silver_limpeza.py` | 🥈 Silver | Limpeza, remoção de leakage, Feature Engineering |
| 3 | `03_gold_analitico.py` | 🥇 Gold | Agregações de negócio, tabela para ML, KPIs |

### Como importar no Databricks:
1. Abra o Databricks Community Edition: https://community.cloud.databricks.com
2. Vá em **Workspace → Import**
3. Selecione "Import from file"
4. Importe cada `.py` desta pasta
5. Rode na ordem: `01` → `02` → `03`

---

## 🥉 Camada Bronze

**Objetivo:** Preservar os dados brutos exatamente como recebidos.

- Formato: **Parquet**
- Transformações: **Nenhuma** (apenas adiciona metadados de ingestão)
- Dados preservados: Inclui colunas de leakage (`parcelas_pagas_ate_3m`, `status_apos_90d`) para fins de auditoria
- Particionamento: Nenhum (tamanho pequeno)

```python
# Exemplo de código Bronze
df_credito.withColumn("_bronze_timestamp", F.lit(timestamp)) \
          .write.mode("overwrite").parquet("s3a://payflow-risk-lake/bronze/payflow_credito/")
```

---

## 🥈 Camada Silver

**Objetivo:** Dados limpos, confiáveis e com features criadas.

Transformações aplicadas:

| Operação | Detalhe |
|----------|---------|
| **Remoção de Leakage** | Remove `parcelas_pagas_ate_3m` e `status_apos_90d` |
| **Imputação de Nulos** | Mediana (robusto a outliers) para todas as variáveis numéricas |
| **Feature Engineering** | `comprometimento_renda = valor_solicitado / renda_mensal` |
| **Segmentação de Risco** | `faixa_score`: BAIXO_RISCO / MEDIO_RISCO / ALTO_RISCO / CRITICO |
| **Flag de Atraso** | `flag_atraso_critico = 1` se `dias_atraso_max_12m > 30` |
| **Filtro de Qualidade** | Remove registros fora de range válido |

**Particionamento:** `faixa_score` (Crédito) e `categoria_produto` (NPS)

---

## 🥇 Camada Gold

**Objetivo:** Tabelas prontas para consumo — modelo ML e Dashboard.

4 tabelas criadas:

### `gold/modelo_features/`
Features limpas para entrada no Random Forest. Inclui `score_propensao_default` (probabilidade calculada por regra de negócio).

### `gold/metricas_risco/`
KPIs agregados por segmento de score:
- `taxa_default_pct` — taxa de inadimplência
- `comprometimento_medio` — comprometimento de renda médio
- `atraso_medio_dias` — dias de atraso médio

### `gold/metricas_nps/`
KPIs por categoria de produto:
- `taxa_detracao_pct` — % de detratores
- `nota_media` — avaliação média
- `atraso_medio` — dias de atraso médio

### `gold/dashboard_resumo/`
Linha única com todos os KPIs executivos do projeto.

---

## ☁️ AWS Academy — S3

### Criar o bucket:
```bash
# Via AWS CLI (no terminal do Learner Lab)
aws s3 mb s3://payflow-risk-lake --region us-east-1
```

### Estrutura de pastas:
```bash
aws s3 ls s3://payflow-risk-lake/ --recursive
```

### Credenciais (renovadas a cada sessão do Learner Lab):
```bash
# Defina no terminal antes de rodar o Streamlit:
set AWS_ACCESS_KEY_ID=ASIA...
set AWS_SECRET_ACCESS_KEY=...
set AWS_SESSION_TOKEN=...
set S3_BUCKET=payflow-risk-lake
```

---

## 🔗 Integração com o Dashboard

O arquivo `src/data_loader_aws.py` faz a ponte entre o S3 e o Dashboard Streamlit:

```python
from src.data_loader_aws import carregar_dados_credito, carregar_kpis_resumo

# Tenta carregar do S3, com fallback para dados locais
df_credito = carregar_dados_credito()
kpis       = carregar_kpis_resumo()
```

---

## 📊 Tecnologias

| Tecnologia | Uso |
|-----------|-----|
| **Databricks Community Edition** | Execução dos notebooks PySpark |
| **Apache Spark (PySpark)** | Processamento distribuído |
| **Delta Lake / Parquet** | Formato colunar eficiente |
| **AWS S3 (Academy)** | Data Lake na nuvem |
| **boto3** | SDK Python para AWS |
| **Streamlit** | Dashboard de consumo |
| **Scikit-Learn** | Modelo Random Forest |
