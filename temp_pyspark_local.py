import pandas as pd
import os

print("Iniciando Processamento de Big Data (Versao Local - Pandas)...")

try:
    print("1 - [SILVER] Lendo os dados brutos da Camada Bronze (270MB+)...")
    # Lê os arquivos pesados (ignorando os que possam estar faltando apenas para o teste)
    df_aluno = pd.read_parquet('data/bronze/TS_ALUNO.parquet')
    df_mun = pd.read_parquet('data/bronze/TS_MUNICIPIO.parquet')
    
    print(f"  -> {len(df_aluno)} registros de alunos carregados!")
    print(f"  -> {len(df_mun)} registros de municípios carregados!")
    
    print("2 - [SILVER] Limpando dados e Agregando proficiencia por Municipio...")
    # Filtra alunos presentes (IN_PRESENCA_LP == 1)
    df_aluno_valido = df_aluno[df_aluno['IN_PRESENCA_LP'] == 1]
    
    # Média de proficiência por município
    df_aluno_agg = df_aluno_valido.groupby('CO_MUNICIPIO').agg(
        proficiencia_media_alunos=('VL_PROFICIENCIA_LP', 'mean'),
        qtd_alunos_avaliados=('ID_ALUNO', 'count')
    ).reset_index()
    
    print("3 - [SILVER] Fazendo o JOIN (Cruzamento) entre Municipios e Alunos...")
    df_integrado = pd.merge(df_mun, df_aluno_agg, on='CO_MUNICIPIO', how='left')
    
    print("4 - [GOLD] Aplicando a Regra de Negocio de Alfabetizacao (Corte 743 pts)...")
    # Usa a coluna oficial do Inep PC_ALUNO_ALFABETIZADO se existir, ou VL_MEDIA_LP
    def definir_status(nota):
        if pd.isna(nota): return "Sem Dados"
        return "Meta Atingida (≥ 743 pts SAEB)" if nota >= 743.0 else "Atenção (< 743 pts SAEB)"
        
    df_integrado['status_alfabetizacao'] = df_integrado['VL_MEDIA_LP'].apply(definir_status)
    
    print("5 - [GOLD] Preparando tabela final para o Dashboard e MongoDB...")
    df_analitico = pd.DataFrame({
        'id_municipio': df_integrado['CO_MUNICIPIO'],
        'nome_municipio': df_integrado['NO_MUNICIPIO'],
        'sigla_uf': df_integrado['SG_UF'],
        'taxa_alfabetizacao': df_integrado['PC_ALUNO_ALFABETIZADO'],
        'qtd_alunos_avaliados': df_integrado['qtd_alunos_avaliados'].fillna(0).astype(int),
        'status_alfabetizacao': df_integrado['status_alfabetizacao'],
        'vulnerabilidade_social': 'Não Informada' # Placeholder caso Inep não tenha
    })
    
    # Remove duplicados
    df_analitico = df_analitico.drop_duplicates(subset=['id_municipio'])
    
    os.makedirs('data/gold', exist_ok=True)
    df_analitico.to_parquet('data/gold/indicador_alfabetizacao.parquet', index=False)
    
    print("SUCESSO! Camada Ouro (Gold) gerada com dados reais e salva no seu Windows!")
    print(f"Total de Municípios processados: {len(df_analitico)}")
    print("Agora voce ja pode rodar o script do Caio ou o seu Dashboard para ver os graficos!")

except Exception as e:
    print(f"Erro durante o processamento: {e}")
