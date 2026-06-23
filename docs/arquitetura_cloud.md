# Arquitetura Híbrida em Nuvem e Medallion
## 1. Escolha da Nuvem: AWS (Amazon Web Services)
Escolhemos a AWS pela sua robustez e facilidade de integração de serviços de Big Data.
- *Armazenamento (Data Lake)*: S3 (Simple Storage Service) hospedando as camadas Bronze, Silver e Gold em formato Parquet particionado.
- *Processamento Batch/Streaming*: AWS EMR e AWS Glue rodando nossos scripts PySpark.
- *Banco de Dados Analítico*: Amazon DocumentDB (compatível com MongoDB) servindo os dados da camada Gold para o Dashboard.
## 2. FinOps - Otimização de Custos
Atendendo aos requisitos de FinOps, adotamos:
- *Armazenamento Parquet Particionado*: O uso do formato colunar .parquet no PySpark reduz o tamanho dos dados armazenados no S3 em até 80%, diminuindo severamente o custo de Storage.
- *Controle de Instâncias PySpark: Limitamos as *shuffle partitions no EMR para garantir eficiência sem desperdício de memória RAM.
## 3. Justificativa Arquitetural (Medallion)
- *Bronze*: Dados brutos do INEP mantendo o histórico sem transformações.
- *Silver*: Realizados JOINs entre as tabelas de Alunos, Municípios e Metas, além de limpeza de nulos.
- *Gold*: Regras de negócio de alfabetização (Nota >= 743 pts), consolidação final e injeção no MongoDB para a tela final (Dashboard).
