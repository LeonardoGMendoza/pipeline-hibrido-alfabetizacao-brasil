import pandas as pd
from pymongo import MongoClient
import os

class MongoDBConnector:
    def __init__(self, uri="mongodb://187.77.32.137:27017/", db_name="analytics_alfabetizacao"):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        
    def carregar_gold_para_mongo(self, parquet_path='data/gold/indicador_alfabetizacao.parquet'):
        print("Conectando ao MongoDB no Servidor Ubuntu...")
        collection = self.db['municipios_completo'] # Usando nova coleção para os dados das 6 tabelas
        
        # Limpa os dados anteriores para garantir atualização limpa
        collection.delete_many({})
        
        if not os.path.exists(parquet_path):
            print(f"Erro: Arquivo {parquet_path} não encontrado. Rode o pipeline PySpark primeiro.")
            return False
            
        # Lendo os dados finais do pipeline PySpark
        print("Lendo os dados processados pelo PySpark (Camada Gold)...")
        df = pd.read_parquet(parquet_path)
        
        registros = df.to_dict('records')
        collection.insert_many(registros)
        print(f"Sucesso! A ponte funcionou: {len(registros)} documentos foram inseridos na coleção NoSQL do seu servidor Ubuntu.")
        return True

if __name__ == '__main__':
    mongo = MongoDBConnector()
    mongo.carregar_gold_para_mongo()