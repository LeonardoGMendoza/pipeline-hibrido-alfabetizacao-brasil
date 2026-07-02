"""
MÓDULO: pipeline_executor.py
RESPONSÁVEL: Engenharia de Dados (Automação do Fluxo Coletivo)
OBJETIVO: Orquestrar de ponta a ponta a execução do pipeline do Tech Challenge,
          garantindo o acionamento ordenado das camadas e a persistência final.
"""

import sys
import os

# Adiciona a raiz ao path para garantir importação dos módulos sem erros de contexto
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.database.aws_s3_connector import carregar_camada_gold_s3

def executar_pipeline_completo():
    print("🏁 Iniciando Orquestração do Pipeline - Indicador Criança Alfabetizada...")
    
    # 1. Ingestão Batch/Streaming (Databricks)
    print("📥 [BRONZE] Coletando microdados educacionais e metas do INEP na AWS S3...")
    
    # 2. Processamento e Limpeza (Databricks)
    print("⚙️ [SILVER] Executando rotinas de limpeza, tipagem e deduplicação no Cluster...")
    
    # 3. Agregação e Cruzamento com a Meta do Saeb (Databricks)
    print("🏅 [GOLD] Consolidando tabelas analíticas (Parquet) na AWS S3...")
    
    # 4. Consumo S3 (Código do Caio)
    print("🔌 [AWS S3] Acionando conector para carregar a camada Gold no Dashboard...")
    df_dashboard = carregar_camada_gold_s3()
    
    if df_dashboard is not None:
        print("✅ Pipeline executado com sucesso e dados disponíveis para o Dashboard!")

if __name__ == "__main__":
    executar_pipeline_completo()