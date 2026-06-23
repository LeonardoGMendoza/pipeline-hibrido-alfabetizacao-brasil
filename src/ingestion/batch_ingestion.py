import os
import shutil
import pandas as pd

class BatchIngestionReal:
    def __init__(self):
        self.raw_dir = 'data/raw/'
        self.bronze_dir = 'data/bronze/'
        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(self.bronze_dir, exist_ok=True)
        
        # Caminhos dos arquivos reais no Windows do usuário
        self.source_dir = r"C:\Users\Leonardo\Downloads"
        self.micro_dir = os.path.join(self.source_dir, "microdados_AEEB_2025", "DADOS")
        
    def copy_to_raw(self):
        print("[RAW] Copiando arquivos originais do INEP...")
        files_to_copy = [
            (os.path.join(self.source_dir, "resultados_e_metas_municipios_2025_v2.xlsx"), "metas_municipios.xlsx"),
            (os.path.join(self.source_dir, "resultados_e_metas_ufs_2025_v1.xlsx"), "metas_ufs.xlsx"),
            (os.path.join(self.micro_dir, "TS_ALUNO.csv"), "TS_ALUNO.csv"),
            (os.path.join(self.micro_dir, "TS_MUNICIPIO.csv"), "TS_MUNICIPIO.csv"),
            (os.path.join(self.micro_dir, "TS_ESTADO.csv"), "TS_ESTADO.csv")
        ]
        
        for src, dest_name in files_to_copy:
            dest = os.path.join(self.raw_dir, dest_name)
            if os.path.exists(src):
                shutil.copy2(src, dest)
                print(f"  -> {dest_name} copiado com sucesso!")
            else:
                print(f"  ⚠️ Arquivo não encontrado: {src}")

    def convert_to_bronze(self):
        print("\n[BRONZE] Convertendo Raw para Parquet (Eficiencia FinOps)...")
        
        def safe_to_parquet(df, path):
            # Converte colunas 'object' para string para evitar ArrowTypeError
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].astype(str)
            df.to_parquet(path, index=False)

        # 1. Metas Municipios (Excel)
        if os.path.exists(f"{self.raw_dir}metas_municipios.xlsx"):
            try:
                print("  Convertendo Metas Municipios...")
                df_metas_mun = pd.read_excel(f"{self.raw_dir}metas_municipios.xlsx")
                safe_to_parquet(df_metas_mun, f"{self.bronze_dir}metas_municipios.parquet")
            except Exception as e: print(f"Erro em Metas Municipios: {e}")
            
        # 2. Metas UFs (Excel)
        if os.path.exists(f"{self.raw_dir}metas_ufs.xlsx"):
            try:
                print("  Convertendo Metas UFs...")
                df_metas_ufs = pd.read_excel(f"{self.raw_dir}metas_ufs.xlsx")
                safe_to_parquet(df_metas_ufs, f"{self.bronze_dir}metas_ufs.parquet")
            except Exception as e: print(f"Erro em Metas UFs: {e}")
            
        # 3. TS_ALUNO
        if os.path.exists(f"{self.raw_dir}TS_ALUNO.csv"):
            try:
                print("  Convertendo TS_ALUNO (Microdados pesados)... aguarde...")
                df_aluno = pd.read_csv(f"{self.raw_dir}TS_ALUNO.csv", sep=';', encoding='latin1', low_memory=False)
                safe_to_parquet(df_aluno, f"{self.bronze_dir}TS_ALUNO.parquet")
            except Exception as e: print(f"Erro em TS_ALUNO: {e}")
            
        # 4. TS_MUNICIPIO
        if os.path.exists(f"{self.raw_dir}TS_MUNICIPIO.csv"):
            try:
                print("  Convertendo TS_MUNICIPIO...")
                df_mun = pd.read_csv(f"{self.raw_dir}TS_MUNICIPIO.csv", sep=';', encoding='latin1', low_memory=False)
                safe_to_parquet(df_mun, f"{self.bronze_dir}TS_MUNICIPIO.parquet")
            except Exception as e: print(f"Erro em TS_MUNICIPIO: {e}")

        # 5. TS_ESTADO
        if os.path.exists(f"{self.raw_dir}TS_ESTADO.csv"):
            try:
                print("  Convertendo TS_ESTADO...")
                df_est = pd.read_csv(f"{self.raw_dir}TS_ESTADO.csv", sep=';', encoding='latin1', low_memory=False)
                safe_to_parquet(df_est, f"{self.bronze_dir}TS_ESTADO.parquet")
            except Exception as e: print(f"Erro em TS_ESTADO: {e}")
            
        print("[BRONZE] Todos os arquivos convertidos para Parquet com sucesso!")

if __name__ == '__main__':
    ingestor = BatchIngestionReal()
    ingestor.copy_to_raw()
    ingestor.convert_to_bronze()
