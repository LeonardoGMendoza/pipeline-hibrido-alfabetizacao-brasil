import pymongo
import json
import os
import pandas as pd

def salvar_camada_gold_nosql(df_gold, connection_string, collection_name="municipios_completo"):
    """
    Função desenvolvida por Caio.
    Recebe um DataFrame (idealmente PySpark, mas adaptado aqui para dicionários/Pandas)
    e persiste na camada Gold do MongoDB.
    """
    try:
        print("🔌 Conectando ao MongoDB...")
        client = pymongo.MongoClient(connection_string)
        db = client.get_database() # Pega o banco da string de conexão
        colecao = db[collection_name]
        
        print(f"🗑️ Limpando coleção anterior ({collection_name})...")
        colecao.delete_many({})
        
        print(f"💾 Inserindo {len(df_gold)} registros na camada NoSQL...")
        # Se for um dataframe Pandas, convertemos para dict
        if hasattr(df_gold, "to_dict"):
            records = df_gold.to_dict("records")
        else:
            records = df_gold
            
        colecao.insert_many(records)
        print("✅ Dados persistidos no MongoDB com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro ao salvar no MongoDB: {e}")

# Script para popular o banco localmente (Simulando o fim do Pipeline PySpark)
if __name__ == "__main__":
    # Pegando o caminho do parquet da Gold
    caminho_gold = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "gold", "indicador_alfabetizacao.parquet")
    
    if os.path.exists(caminho_gold):
        print(f"Lendo dados processados (Camada Gold): {caminho_gold}")
        df = pd.read_parquet(caminho_gold)
        
        # Conexão com o IP do servidor Ubuntu incluindo o database no final da URI
        conexao = "mongodb://187.77.32.137:27017/analytics_alfabetizacao"
        salvar_camada_gold_nosql(df, conexao)
    else:
        print("❌ Arquivo Parquet da camada Gold não encontrado. Execute o pipeline local primeiro.")