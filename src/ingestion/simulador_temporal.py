import pandas as pd
import numpy as np
import os
import sys

sys.stdout.reconfigure(encoding='utf-8')

print("🚀 Iniciando Simulador Temporal de Dados (2023-2026)...")

# Mesmo setup do Databricks (Estrutura original, dados próximos)
np.random.seed(42)
n_alunos = 10000

df_mun_bronze = pd.DataFrame({
    "CO_MUNICIPIO": [3550308, 3304557, 3106200, 4106902, 4314902],
    "NO_MUNICIPIO": ["São Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba", "Porto Alegre"],
    "SG_UF": ["SP", "RJ", "MG", "PR", "RS"]
})
codigos_mun = df_mun_bronze["CO_MUNICIPIO"].tolist()

def gerar_dados_ano(ano, media_proficiencia):
    # BRONZE: Ingestão de dados brutos
    df_aluno = pd.DataFrame({
        "ID_ALUNO": range(1, n_alunos + 1),
        "CO_MUNICIPIO": np.random.choice(codigos_mun, n_alunos),
        "IN_PRESENCA_LP": np.random.choice([0, 1], n_alunos, p=[0.05, 0.95]),
        "VL_PROFICIENCIA_LP": np.random.normal(media_proficiencia, 50, n_alunos),
        "ANO": ano
    })
    # Sujeira INEP: faltantes não tem nota
    df_aluno.loc[df_aluno["IN_PRESENCA_LP"] == 0, "VL_PROFICIENCIA_LP"] = np.nan
    
    # SILVER: Limpeza e JOIN
    df_silver = df_aluno[df_aluno["IN_PRESENCA_LP"] == 1].copy()
    
    df_agg = df_silver.groupby(["CO_MUNICIPIO", "ANO"]).agg(
        proficiencia_media_alunos = ("VL_PROFICIENCIA_LP", "mean"),
        qtd_alunos_avaliados = ("ID_ALUNO", "count")
    ).reset_index()
    
    df_integrado = pd.merge(df_mun_bronze, df_agg, on="CO_MUNICIPIO", how="right")
    
    # GOLD: Regras de Negócio
    def definir_status(nota):
        if pd.isna(nota): return "Sem Dados"
        return "Meta Atingida (≥ 743 pts)" if nota >= 743.0 else "Atenção (< 743 pts)"
        
    df_gold = pd.DataFrame({
        'id_municipio': df_integrado['CO_MUNICIPIO'],
        'nome_municipio': df_integrado['NO_MUNICIPIO'],
        'sigla_uf': df_integrado['SG_UF'],
        'qtd_alunos_avaliados': df_integrado['qtd_alunos_avaliados'].fillna(0).astype(int),
        'proficiencia_media': df_integrado['proficiencia_media_alunos'].round(2),
        'status_alfabetizacao': df_integrado['proficiencia_media_alunos'].apply(definir_status),
        'ano': df_integrado['ANO']
    })
    return df_gold

# Cenário de Simulação (Batch Histórico + Streaming Novo)
# 2023: 750 (Meta atingida com folga)
# 2024: 745 (Caindo levemente)
# 2025: 740 (Alerta: Caiu abaixo da meta de 743)
# 2026: 735 (Simulação do dado NOVO de Streaming: Cenário Crítico)

anos_config = {
    2023: 750,
    2024: 745,
    2025: 740,
    2026: 735
}

dfs_gold = []
for ano, media in anos_config.items():
    print(f"📊 Processando Ano {ano} (Média Base INEP: {media})...")
    df_gold_ano = gerar_dados_ano(ano, media)
    dfs_gold.append(df_gold_ano)

# Junta todos os anos
df_final_gold = pd.concat(dfs_gold, ignore_index=True)

# Salvar arquivo Parquet final localmente
pasta_saida = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "gold")
os.makedirs(pasta_saida, exist_ok=True)
caminho_arquivo = os.path.join(pasta_saida, "indicador_alfabetizacao_temporal.parquet")

df_final_gold.to_parquet(caminho_arquivo, index=False)

print(f"\n✅ Simulação concluída com SUCESSO!")
print(f"💾 Arquivo gerado em: {caminho_arquivo}")
print("\n🔍 Evolução da Proficiência Média no Brasil (2023-2026):")
print(df_final_gold.groupby("ano")["proficiencia_media"].mean().round(2))
