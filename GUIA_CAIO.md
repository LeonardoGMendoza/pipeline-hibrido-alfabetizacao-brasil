# 🚀 Guia do Caio — Databricks + AWS (Alfabetização Brasil)

> **Projeto:** Pipeline Híbrido Alfabetização (INEP)

---

## PASSO 1 — AWS Academy (S3)
1. No **AWS Academy Learner Lab**, clique em **"Start Lab"**.
2. Abra o Console AWS → **S3** → Crie o bucket: `alfabetizacao-datalake` (Region `us-east-1`).
3. Volte ao Learner Lab → Clique em **"AWS Details"** → **"Show"**.
4. Copie as 3 credenciais (Access Key, Secret Key e Session Token).

---

## PASSO 2 — Databricks (Execução do Pipeline)
1. Abra o **Databricks Community Edition**.
2. Importe o notebook principal clicando em Workspace → Import → URL:
   ```
   https://raw.githubusercontent.com/LeonardoGMendoza/pipeline-hibrido-alfabetizacao-brasil/main/databricks/00_pipeline_completo.py
   ```
3. Cole as 3 credenciais da AWS copiadas na **Célula 2** do notebook.
4. Clique em **"Run All"**.
   - *Isso vai gerar e salvar as camadas Bronze, Silver e Gold no S3 automaticamente.*

---

## PASSO 3 — Databricks (Quality Gate)
1. Importe o notebook de validação de qualidade:
   ```
   https://raw.githubusercontent.com/LeonardoGMendoza/pipeline-hibrido-alfabetizacao-brasil/main/databricks/04_quality_checks.py
   ```
2. Cole as mesmas 3 credenciais da AWS na **Célula 2**.
3. Clique em **"Run All"**.
   - *O resultado final deve ser `🟢 QUALITY GATE APROVADO! Score: 100%`.*

---

## 📸 PASSO 4 — Prints para o Pitch Final
Tire print destas 3 telas para comprovar a arquitetura Medallion em Cloud:
1. **S3 Console:** Mostrando as pastas `bronze`, `silver` e `gold` no bucket.
2. **Databricks 00:** Mostrando o sucesso final do pipeline (`PIPELINE INEP MEDALLION EXECUTADO`).
3. **Databricks 04:** Mostrando a aprovação do Quality Gate (`Score 100%`).
