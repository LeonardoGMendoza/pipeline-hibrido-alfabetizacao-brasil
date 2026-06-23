import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Alfabetiza Brasil Analytics", layout="wide", initial_sidebar_state="expanded")

# --- CONEXÃO COM A CAMADA GOLD (Local Parquet) ---
@st.cache_data(ttl=600)
def carregar_dados():
    try:
        # Lê do arquivo local gerado pelo pipeline
        return pd.read_parquet('data/gold/indicador_alfabetizacao.parquet')
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return pd.DataFrame()

df = carregar_dados()

# --- INTERFACE ---
st.title("📚 Dashboard Nacional: Criança Alfabetizada")
st.markdown("Monitoramento das metas do INEP e Base dos Dados - Fase 2")

if df.empty:
    st.warning("⚠️ Aguardando dados da Engenharia de Dados (Pipeline PySpark).")
else:
    # --- BARRA LATERAL (Filtros Novos e Antigos) ---
    st.sidebar.header("Filtros Estratégicos")
    
    # Filtro de Estado (UF)
    lista_ufs = ["Todos"] + sorted(df['sigla_uf'].dropna().unique().tolist())
    uf_selecionada = st.sidebar.selectbox("Filtrar por Estado (UF):", lista_ufs)
    
    # Filtro de Status
    lista_status = ["Todos"] + sorted(df['status_alfabetizacao'].dropna().unique().tolist())
    status_selecionado = st.sidebar.selectbox("Filtrar por Status Saeb:", lista_status)
    
    # Filtro de Vulnerabilidade
    lista_vuln = ["Todas"] + sorted(df['vulnerabilidade_social'].dropna().unique().tolist())
    vuln_selecionada = st.sidebar.selectbox("Nível de Vulnerabilidade Social:", lista_vuln)
    
    # Aplica os filtros
    if uf_selecionada != "Todos":
        df = df[df['sigla_uf'] == uf_selecionada]
    if status_selecionado != "Todos":
        df = df[df['status_alfabetizacao'] == status_selecionado]
    if vuln_selecionada != "Todas":
        df = df[df['vulnerabilidade_social'] == vuln_selecionada]

    # --- MÉTRICAS GERAIS ---
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Municípios Analisados", f"{len(df['id_municipio'].unique())}")
    col2.metric("Média de Proficiência", f"{df['taxa_alfabetizacao'].mean():.1f}%")
    col3.metric("Meta Nacional (2024)", "80.0%")
    col4.metric("Camada Ouro", "Local Parquet 🟡")

    # --- GRÁFICOS ---
    st.markdown("---")
    row1_col1, row1_col2 = st.columns(2)

    with row1_col1:
        st.subheader("Proficiência Média por Município")
        fig_bar = px.bar(
            df.sort_values('taxa_alfabetizacao', ascending=False).head(15), 
            x='nome_municipio', 
            y='taxa_alfabetizacao',
            color='status_alfabetizacao',
            color_discrete_map={'Meta Atingida (≥ 743 pts SAEB)': '#2ecc71', 'Atenção (< 743 pts SAEB)': '#e74c3c'},
            labels={'taxa_alfabetizacao': 'Taxa de Alfabetização (%)', 'nome_municipio': 'Município'}
        )
        # Linha da Meta Nacional (exemplo)
        fig_bar.add_hline(y=80.0, line_dash="dash", line_color="yellow", annotation_text="Meta Brasil")
        st.plotly_chart(fig_bar, use_container_width=True)

    with row1_col2:
        st.subheader("Distribuição do Status de Alfabetização")
        fig_pie = px.pie(
            df, 
            names='status_alfabetizacao', 
            hole=0.4,
            color='status_alfabetizacao',
            color_discrete_map={'Meta Atingida (≥ 743 pts SAEB)': '#2ecc71', 'Atenção (< 743 pts SAEB)': '#e74c3c'}
        )
        st.plotly_chart(fig_pie, use_container_width=True)