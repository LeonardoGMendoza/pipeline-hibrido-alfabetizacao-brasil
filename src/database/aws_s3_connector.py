import boto3
import pandas as pd
import io
import os

def carregar_camada_gold_s3(bucket_name="alfabetizacao-datalake", object_key="gold/indicador_alfabetizacao/dados.parquet"):
    """
    Função desenvolvida com a estrutura aprovada, mas focada 100% em AWS (S3).
    Busca o arquivo final (Camada Gold) processado pelo Databricks direto do S3.
    """
    try:
        print(f"☁️ Conectando ao AWS S3 (Bucket: {bucket_name})...")
        
        # O boto3 automaticamente pega as credenciais de ambiente (ou as configuradas)
        s3 = boto3.client("s3")
        
        print(f"📥 Fazendo download do arquivo: {object_key}...")
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        
        print("📊 Convertendo Parquet para DataFrame Pandas...")
        # Lendo o parquet direto da memória (sem precisar salvar arquivo local)
        df_gold = pd.read_parquet(io.BytesIO(response["Body"].read()))
        
        print(f"✅ Dados carregados com sucesso! ({len(df_gold)} registros encontrados)")
        return df_gold
        
    except Exception as e:
        print(f"❌ Erro ao buscar os dados na AWS S3: {e}")
        print("⚠️ Tentando carregar o fallback local (data/gold/indicador_alfabetizacao.parquet)...")
        
        # Fallback de segurança para rodar localmente caso a AWS esteja fora
        caminho_local = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "gold", "indicador_alfabetizacao.parquet")
        if os.path.exists(caminho_local):
            return pd.read_parquet(caminho_local)
        else:
            raise FileNotFoundError("Não foi possível acessar a AWS nem encontrar os dados localmente.")

if __name__ == "__main__":
    # Teste de execução local para o Caio/Leo
    df = carregar_camada_gold_s3()
    if df is not None:
        print("\n🔍 Amostra dos Dados Carregados:")
        print(df.head())
