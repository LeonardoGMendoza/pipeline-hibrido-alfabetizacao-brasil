# Arquitetura e Engenharia de Dados: Alfabetização no Brasil
## 1. Visão Geral (O Desafio de Negócio)
Este projeto estrutura um pipeline de dados escalável para processar, em nuvem, os dados educacionais da plataforma "Base dos Dados", com foco no "Indicador Criança Alfabetizada". O objetivo é apoiar gestores educacionais na visualização rápida e precisa de municípios e escolas que necessitam de intervenção ou repasse de fundos governamentais (Fundeb).
## 2. Metodologia CRISP-DM Aplicada
Adotamos o ciclo CRISP-DM para garantir que a engenharia de dados estivesse alinhada à realidade escolar:
1. *Entendimento do Negócio*: Compreender a meta nacional do Saeb (ponto de corte de 743 pontos para considerar a criança alfabetizada).
2. *Entendimento dos Dados*: Mapeamento das tabelas de Município, UF e Resultados de proficiência da Base dos Dados.
3. *Preparação dos Dados*: (O Motor do Projeto) Uso do Apache Spark (PySpark) para limpar valores nulos, tratar strings e juntar tabelas espalhadas num grande Dataset Fato.
4. *Modelagem / Analytics*: Geração de features de agregação e categorização (ex: "Alfabetizado" vs "Atenção").
5. *Avaliação*: Garantia de Qualidade de Dados (Data Quality) garantindo valores entre 0 e 100 ou limites de pontuação.
6. *Deployment*: Persistência no MongoDB (Caio) e orquestração de um Dashboard Interativo em Streamlit (Reinaldo).
## 3. Arquitetura de Pipeline (Medallion)
Escolhemos a arquitetura de *Data Lakehouse* baseada em camadas Medallion para maximizar a governança:
- *Camada Bronze (Raw)*: Recebe os dados de ingestão batch/streaming na sua forma original. Preserva 100% do histórico, atua como "Landing Zone". Permite reprocessamento caso o pipeline falhe. Formato otimizado: Parquet.
  
- *Camada Silver (Cleansed)*: O "Motor" do PySpark entra em ação. Nulos são removidos, duplicatas são expurgadas, chaves primárias (id_municipio) são padronizadas e validadas. A tabela ganha consistência matemática.
- *Camada Gold (Analytics)*: Os dados agora estão agregados pelo escopo de negócio. O PySpark realiza os Joins e group-bys finais, criando a tabela final de indicadores de alfabetização por estado e município.
## 4. Persistência NoSQL e Desacoplamento
Uma decisão crítica de arquitetura foi desacoplar o motor de cálculo (Spark) do motor de visualização (App). Para isso, conectamos o PySpark nativamente a um banco *MongoDB*. O banco absorve os dados Gold, entregando respostas indexadas em milissegundos para o Dashboard em Streamlit.
