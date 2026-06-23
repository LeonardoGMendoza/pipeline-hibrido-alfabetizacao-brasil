import os
import pandas as pd
import numpy as np

class BatchIngestion:
    def __init__(self, output_dir='data/bronze/'):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        
    def mock_inep_datasets(self):
        print("📥 [BATCH] Simulando extração das 6 entidades da Base dos Dados...")
        
        # 1. UF
        df_uf = pd.DataFrame({
            'sigla_uf': ['SP', 'RJ', 'MG'],
            'nome': ['São Paulo', 'Rio de Janeiro', 'Minas Gerais'],
            'regiao': ['Sudeste', 'Sudeste', 'Sudeste']
        })
        
        # 2. Município
        df_mun = pd.DataFrame({
            'id_municipio': [3550308, 3304557, 3106200],
            'nome': ['São Paulo', 'Rio de Janeiro', 'Belo Horizonte'],
            'sigla_uf': ['SP', 'RJ', 'MG']
        })
        
        # 3. Meta Alfabetização Brasil
        df_meta_br = pd.DataFrame({
            'ano': [2024, 2024, 2024],
            'meta_nacional': [80.0, 80.0, 80.0]
        })
        
        # 4. Meta Alfabetização UF (Conforme schema do Inep passado pelo usuário)
        df_meta_uf = pd.DataFrame({
            'ano': [2024, 2024, 2024],
            'sigla_uf': ['SP', 'RJ', 'MG'],
            'rede': ['Estadual', 'Estadual', 'Estadual'],
            'taxa_alfabetizacao': [75.0, 70.0, 72.0],
            'media_portugues': [745.0, 740.0, 743.0]
        })
        
        # 5. Meta Alfabetização Município
        df_meta_mun = pd.DataFrame({
            'ano': [2024, 2024, 2024],
            'id_municipio': [3550308, 3304557, 3106200],
            'rede': ['Municipal', 'Municipal', 'Municipal'],
            'taxa_alfabetizacao': [78.0, 71.0, 73.0]
        })
        
        # 6. Dados de alunos (Simulado via microdados agregados)
        np.random.seed(42)
        df_alunos = pd.DataFrame({
            'id_municipio': [3550308, 3304557, 3106200],
            'qtd_alunos_avaliados': np.random.randint(1000, 50000, 3),
            'proporcao_aluno_nivel_4': [0.4, 0.3, 0.35], # Níveis mais altos indicam alfabetização
            'vulnerabilidade_social': ['Alta', 'Média', 'Baixa']
        })

        # Salva em Parquet
        df_uf.to_parquet(f'{self.output_dir}/uf.parquet', index=False)
        df_mun.to_parquet(f'{self.output_dir}/municipio.parquet', index=False)
        df_meta_br.to_parquet(f'{self.output_dir}/meta_brasil.parquet', index=False)
        df_meta_uf.to_parquet(f'{self.output_dir}/meta_uf.parquet', index=False)
        df_meta_mun.to_parquet(f'{self.output_dir}/meta_municipio.parquet', index=False)
        df_alunos.to_parquet(f'{self.output_dir}/alunos.parquet', index=False)
        
        print("✅ [BATCH] Arquivos salvos com sucesso na camada Bronze!")

if __name__ == '__main__':
    ingestor = BatchIngestion()
    ingestor.mock_inep_datasets()
