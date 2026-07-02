# 💰 FinOps — Gestão de Custos Cloud
## Projeto Alfabetização Brasil (INEP)

Este documento detalha as estratégias de redução de custos (FinOps) implementadas no pipeline de Big Data educacional do INEP.

---

## 📊 O Desafio: Volume de Dados
O microdado bruto de alunos do INEP original pesa mais de **270 MB (aprox. 2.2 milhões de registros)**. Processar isso de forma ineficiente gera custos altos de armazenamento e computação.

---

## 🏗️ Estratégias FinOps Implementadas

### 1. Compressão Parquet
- **CSV Bruto:** ~270 MB
- **Parquet (Bronze):** ~35 MB (Redução de 87%)
- **Impacto:** O Amazon S3 cobra por GB armazenado. Usando formato colunar Parquet, reduzimos o custo de armazenamento da camada Bronze em quase 90%.

### 2. Arquitetura Medallion Otimizada
O processamento intensivo (Filtro `IN_PRESENCA_LP == 1` e a agregação por `CO_MUNICIPIO`) ocorre **apenas uma vez** na transição da camada Bronze para a Silver.
- **Silver:** Reduz 2.2 milhões de alunos para ~5.500 registros de municípios.
- **Impacto:** O Banco de Dados NoSQL de consumo (MongoDB) recebe apenas a tabela Gold agregada (alguns Kilobytes). O servidor Ubuntu pode rodar em uma instância super barata (ex: t3.micro) porque não precisa processar o Big Data em tempo real.

### 3. Computação Efêmera (Databricks)
O pipeline é desenhado para rodar em *Batch* (lotes).
- O Cluster Databricks liga, processa os dados, grava no S3 e é programado com **Auto-terminate** para desligar após o job.
- Evita o desperdício de "idle clusters" que drenam orçamento na AWS.

### 4. Armazenamento Inteligente
- Dados Gold (consultados pelo dashboard) ficam em S3 Standard / MongoDB.
- Dados Bronze e Silver (processados raramente) podem ser movidos via Lifecycle Rule para S3 Glacier.

---

## 📉 Custo Estimado na Nuvem

| Componente | Uso / Mês | Custo Estimado |
|------------|-----------|----------------|
| **Databricks CE** | Processamento Batch | Gratuito |
| **AWS S3** | < 1 GB Armazenamento | ~$0.05 / mês |
| **MongoDB (Ubuntu)** | EC2 t3.micro | ~$8.00 / mês |
| **Total** | Operação Enxuta | **~$8.05 / mês** |

> *Nota: Através do AWS Academy, o custo para o desenvolvimento foi mantido em US$ 0,00 utilizando os créditos estudantis.*
