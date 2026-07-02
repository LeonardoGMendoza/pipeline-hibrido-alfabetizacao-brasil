# 🚀 Alfabetização Brasil
## Pitch de Vendas & Apresentação Técnica
### FIAP AI Scientist Pós-Tech

---

## 👥 Grupo
| Nome | RM | Papel |
|------|-----|-------|
| Leonardo Jr. G. Mendoza | RM 373713 | Engenharia de Dados & Pipeline |
| Caio Morais Rubino | RM 371492 | Infraestrutura Cloud |
| Winny Tavares | RM 371471 | Governança & FinOps |
| Reinaldo Fernandes | RM 371717 | Frontend & Analytics |

---

# 🎯 PITCH DE VENDAS

## O Desafio da Alfabetização
No Brasil, acompanhar a efetividade da alfabetização infantil nas escolas públicas é um desafio monumental.
O MEC e o INEP possuem microdados excelentes, mas são:
- Pesados demais (Gigabytes de CSVs).
- Difíceis de cruzar e interpretar.
- Isolados, dificultando a tomada de decisão rápida por prefeitos e governadores.

## A Solução
Um **Data Lakehouse Governamental** que transforma milhões de avaliações brutas de alunos em um painel executivo veloz e intuitivo.
Nossa solução automatiza todo o ciclo: da coleta do dado do aluno (INEP) até o mapa interativo no celular do Secretário de Educação.

### Por que comprar essa arquitetura?
1. **Escalabilidade (AWS + Databricks):** Não importa se temos 1 milhão ou 50 milhões de alunos, o pipeline processa tudo em nuvem.
2. **Custo Baixo (FinOps):** Redução de 87% do volume de dados convertendo tudo para Parquet e processando apenas o agregado final.
3. **Qualidade Garantida:** Sistema possui um "Quality Gate" automatizado que impede que dados errados poluam o Dashboard.
4. **Alta Performance:** O consumo dos dados não acontece no Lake pesado, mas sim em um banco NoSQL (MongoDB) ultra-rápido.

---

# 🔬 APRESENTAÇÃO TÉCNICA

## Metodologia CRISP-DM

1. **Business Understanding:** Foco na métrica oficial do INEP (Corte: 743 pontos no Saeb = Alfabetizado).
2. **Data Understanding:** Base do INEP (`TS_ALUNO`), com 2.2 milhões de registros. Entendimento da chave `CO_MUNICIPIO`.
3. **Data Preparation:** Filtrar alunos ausentes na prova, tratar nulos, agrupar e calcular média municipal.
4. **Modeling (Data):** Arquitetura Medallion:
   - *Bronze:* Dados crus (Landing Zone).
   - *Silver:* Limpeza e cálculo de médias.
   - *Gold:* Junção final, regras de negócio e tabela analítica leve.
5. **Evaluation:** Validação de Qualidade de Dados (Great Expectations), verificando unicidade, presença de nulos e validade das pontuações.
6. **Deployment:** MongoDB (Servidor Ubuntu) e Dashboard Web (Streamlit).

## Arquitetura Cloud Híbrida

- **Processamento Batch:** AWS S3 + Databricks Apache Spark.
- **Serving Layer:** MongoDB.
- **Visualização:** Streamlit Intergaláctico.

## Gestão de Qualidade

Nosso `04_quality_checks.py` roda as seguintes baterias de testes:
- **Unicidade:** Garante 1 linha por município.
- **Completude:** Garante que nenhum município ficou sem o rótulo de "Status Alfabetização".
- **Limites Matemáticos:** As pontuações e quantidades não podem ser menores que 0.

---
*Alfabetização Brasil — Entregando inteligência para o setor público.*
