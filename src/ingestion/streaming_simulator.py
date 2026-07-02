import pandas as pd
import time
import os
from pymongo import MongoClient
import random

print("🚀 Iniciando Simulador de Streaming de Dados Educacionais...")

# Configurações do Banco de Dados na Nuvem (Caio vai fornecer o IP)
# Substitua 'localhost' pelo IP da AWS quando o Caio criar a máquina
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "educacao_db"
COLLECTION_NAME = "streaming_alfabetizacao"

try:
    # Conecta no MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    print("✅ Conectado ao MongoDB com sucesso!")
    
    # Lê a Camada Gold local (O arquivo que você já gerou com o temp_pyspark_local.py)
    caminho_gold = 'data/gold/indicador_alfabetizacao.parquet'
    
    if not os.path.exists(caminho_gold):
        print(f"❌ Arquivo não encontrado: {caminho_gold}. Rode o pipeline Batch primeiro.")
        exit()
        
    df = pd.read_parquet(caminho_gold)
    dados = df.to_dict(orient='records')
    
    print(f"📡 Iniciando transmissão de {len(dados)} registros em tempo real...")
    
    # Loop simulando streaming (1 registro a cada X segundos)
    for index, registro in enumerate(dados):
        # Simula uma atualização "ao vivo"
        registro['timestamp_ingestao'] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
        registro['tipo_ingestao'] = 'STREAMING'
        
        # Insere no banco NoSQL
        collection.insert_one(registro)
        
        print(f"[{registro['timestamp_ingestao']}] 📥 Município: {registro['nome_municipio']} ({registro['sigla_uf']}) processado e salvo no MongoDB.")
        
        # Delay aleatório entre 1 e 3 segundos para parecer dados reais chegando
        time.sleep(random.uniform(1.0, 3.0))

except Exception as e:
    print(f"❌ Erro de conexão ou processamento: {e}")
