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
        print("📥 [RAW] Copiando arquivos originais do INEP...")
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
        print("\n⚙️ [BRONZE] Convertendo Raw para Parquet (Eficiência FinOps)...")
        
        # 1. Metas Municipios (Excel)
        if os.path.exists(f"{self.raw_dir}metas_municipios.xlsx"):
            print("  Convertendo Metas Municipios...")
            df_metas_mun = pd.read_excel(f"{self.raw_dir}metas_municipios.xlsx")
            df_metas_mun.to_parquet(f"{self.bronze_dir}metas_municipios.parquet", index=False)
            
        # 2. Metas UFs (Excel)
        if os.path.exists(f"{self.raw_dir}metas_ufs.xlsx"):
            print("  Convertendo Metas UFs...")
            df_metas_ufs = pd.read_excel(f"{self.raw_dir}metas_ufs.xlsx")
            df_metas_ufs.to_parquet(f"{self.bronze_dir}metas_ufs.parquet", index=False)
            
        # 3. TS_ALUNO (CSV pesado, precisa de chunking ou leitura direta com PyArrow se possível)
        # Por simplicidade e segurança de memória, vamos ler com pandas (se aguentar) ou pyarrow engine
        if os.path.exists(f"{self.raw_dir}TS_ALUNO.csv"):
            print("  Convertendo TS_ALUNO (Microdados pesados)... aguarde...")
            df_aluno = pd.read_csv(f"{self.raw_dir}TS_ALUNO.csv", sep=';', encoding='utf-8', engine='pyarrow')
            df_aluno.to_parquet(f"{self.bronze_dir}TS_ALUNO.parquet", index=False)
            
        # 4. TS_MUNICIPIO
        if os.path.exists(f"{self.raw_dir}TS_MUNICIPIO.csv"):
            print("  Convertendo TS_MUNICIPIO...")
            df_mun = pd.read_csv(f"{self.raw_dir}TS_MUNICIPIO.csv", sep=';', encoding='utf-8')
            df_mun.to_parquet(f"{self.bronze_dir}TS_MUNICIPIO.parquet", index=False)

        # 5. TS_ESTADO
        if os.path.exists(f"{self.raw_dir}TS_ESTADO.csv"):
            print("  Convertendo TS_ESTADO...")
            df_est = pd.read_csv(f"{self.raw_dir}TS_ESTADO.csv", sep=';', encoding='utf-8')
            df_est.to_parquet(f"{self.bronze_dir}TS_ESTADO.parquet", index=False)
            
        print("✅ [BRONZE] Todos os arquivos convertidos para Parquet com sucesso!")

if __name__ == '__main__':
    ingestor = BatchIngestionReal()
    ingestor.copy_to_raw()
    ingestor.convert_to_bronze()
