import pandas as pd
from pymongo import MongoClient

def carregar_dados_para_mongo():
    print("Conectando ao MongoDB no servidor Ubuntu...")
    # Conecta ao MongoDB do seu servidor na porta 27017
    client = MongoClient('mongodb://187.77.32.137:27017/')
    
    # Cria (ou usa) o banco 'analytics_alfabetizacao' e a coleção 'municipios'
    db = client['analytics_alfabetizacao']
    collection = db['municipios']
    
    # Limpa a coleção para não duplicar dados caso rode duas vezes
    collection.delete_many({})
    
    # Lê o parquet que geramos localmente
    print("Lendo arquivo Parquet local...")
    df = pd.read_parquet('data/gold/indicador_alfabetizacao.parquet')
    
    # Converte para dicionário e insere no MongoDB
    registros = df.to_dict('records')
    collection.insert_many(registros)
    
    print(f"Sucesso! {len(registros)} registros inseridos no MongoDB do seu servidor.")

if __name__ == '__main__':
    carregar_dados_para_mongo()
