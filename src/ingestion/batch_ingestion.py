import os
import pandas as pd
class Ingestor:
    def __init__(self):
        self.raw_dir = 'data/raw/'
        self.bronze_dir = 'data/bronze/'
        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(self.bronze_dir, exist_ok=True)
    def convert_to_bronze(self):
        print("\n[BRONZE] Convertendo Raw para Parquet (Eficiencia FinOps)...")
        
        def safe_to_parquet(df, path):
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].astype(str)
            df.to_parquet(path, index=False)
        if os.path.exists(f"{self.raw_dir}metas_municipios.xlsx"):
            try:
                df_metas_mun = pd.read_excel(f"{self.raw_dir}metas_municipios.xlsx")
                safe_to_parquet(df_metas_mun, f"{self.bronze_dir}metas_municipios.parquet")
            except Exception as e: print(f"Erro: {e}")
            
        if os.path.exists(f"{self.raw_dir}TS_ALUNO.csv"):
            try:
                print("  Convertendo TS_ALUNO (Microdados pesados)... aguarde...")
                df_aluno = pd.read_csv(f"{self.raw_dir}TS_ALUNO.csv", sep=';', encoding='latin1', low_memory=False)
                safe_to_parquet(df_aluno, f"{self.bronze_dir}TS_ALUNO.parquet")
            except Exception as e: print(f"Erro: {e}")
            
        if os.path.exists(f"{self.raw_dir}TS_MUNICIPIO.csv"):
            try:
                df_mun = pd.read_csv(f"{self.raw_dir}TS_MUNICIPIO.csv", sep=';', encoding='latin1', low_memory=False)
                safe_to_parquet(df_mun, f"{self.bronze_dir}TS_MUNICIPIO.parquet")
            except Exception as e: print(f"Erro: {e}")
            
        print("[BRONZE] Todos os arquivos convertidos para Parquet com sucesso!")
if __name__ == '__main__':
    ingestor = Ingestor()
    ingestor.convert_to_bronze()
