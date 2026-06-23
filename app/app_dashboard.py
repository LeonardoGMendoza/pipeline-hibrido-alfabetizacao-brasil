import streamlit as st
import pandas as pd
import plotly.express as px
feature/reinaldo-dashboard-starwars
import numpy as np


# ==============================================================================
# 1. ARQUITETURA DA INTERFACE & CONFIGURAÇÕES EXECUTIVAS
# ==============================================================================
st.set_page_config(
    page_title="Data Lakehouse Analytics | Alfabetização Brasil",
    layout="wide",
    page_icon="🏫",
    initial_sidebar_state="expanded"
)

# Estilização CSS customizada para garantir identidade visual sóbria


st.set_page_config(page_title="Alfabetiza Brasil Analytics", layout="wide", initial_sidebar_state="expanded")

# --- CSS CUSTOMIZADO (Tema Intergaláctico / Star Wars & Neon) ---
 main
st.markdown("""
<style>
/* Animação para mover as estrelas continuamente */
@keyframes move-stars {
    from {
        background-position: 0 0, 40px 60px, 130px 270px, 70px 100px;
    }
    to {
        background-position: -550px 550px, -310px 410px, -120px 520px, -80px 250px;
    }
}

/* Fundo Intergaláctico animado */
.stApp {
    background-color: #000000;
    background-image: radial-gradient(white, rgba(255,255,255,.2) 2px, transparent 4px),
                      radial-gradient(white, rgba(255,255,255,.15) 1px, transparent 3px),
                      radial-gradient(white, rgba(255,255,255,.1) 2px, transparent 4px),
                      radial-gradient(rgba(255,255,255,.4), rgba(255,255,255,.1) 2px, transparent 3px);
    background-size: 550px 550px, 350px 350px, 250px 250px, 150px 150px;
    animation: move-stars 150s linear infinite;
    color: #ffffff;
}

/* Cards de Métricas limpos (Sem o efeito Neon/Brilho) */
div[data-testid="metric-container"] {
    background-color: rgba(20, 20, 20, 0.8);
    border: 1px solid #444444;
    border-radius: 10px;
    padding: 15px;
}
div[data-testid="metric-container"] > div {
    color: #ffffff !important;
}

/* Customizar tags do Multiselect (Estilo Vermelho do Stellar) */
span[data-baseweb="tag"] {
    background-color: #ff4b4b !important;
    color: white !important;
    border-radius: 5px !important;
}
span[data-baseweb="tag"] svg {
    fill: white !important;
}

h1, h2, h3, p {
    color: #ffffff !important;
}
</style>
""", unsafe_allow_html=True)

# --- CONEXÃO COM A CAMADA GOLD ---
@st.cache_data(ttl=600)
def carregar_dados():
    try:
        df = pd.read_parquet('data/gold/indicador_alfabetizacao.parquet')
        df['taxa_alfabetizacao'] = pd.to_numeric(df['taxa_alfabetizacao'], errors='coerce')
        return df
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return pd.DataFrame()

feature/reinaldo-dashboard-starwars
df = carregar_pipeline_data()

# ==============================================================================
# 3. SIDEBAR - CONTROLADORES DE FILTROS (Maturidade Analítica)
# ==============================================================================
st.sidebar.markdown("# ⚙️ Painel de Controle")
st.sidebar.title("Filtros Estratégicos")
st.sidebar.markdown("Use os parâmetros abaixo para simular cenários de corte orçamentário e intervenção:")

if not df.empty:
    status_options = df['status_alfabetizacao'].unique()
    vuln_options = df['vulnerabilidade_social'].unique()
else:
    status_options = []
    vuln_options = []

status_selecionado = st.sidebar.multiselect(
    "Filtrar por Status Saeb:",
    options=status_options,
    default=status_options
)

vulnerabilidade_selecionada = st.sidebar.multiselect(
    "Nível de Vulnerabilidade Social:",
    options=vuln_options,
    default=vuln_options
)

# Aplicando os filtros dinamicamente
if not df.empty:
    df_filtrado = df[
        (df['status_alfabetizacao'].isin(status_selecionado)) &
        (df['vulnerabilidade_social'].isin(vulnerabilidade_selecionada))
    ]
else:
    df_filtrado = pd.DataFrame()

# ==============================================================================
# 4. STORYTELLING VISUAL & PAINEL DE CONTROLE EXECUTIVO
# ==============================================================================
st.title("🏫 Monitor de Performance - Compromisso Nacional Criança Alfabetizada")
st.caption("Fase 2: Data Architecture, Pipeline Medallion e Persistência Poliglota NoSQL")
st.markdown("---")

if df_filtrado.empty:
    st.warning("⚠️ Nenhum dado disponível ou localizado com os filtros selecionados.")
else:
    # 📊 Camada de Métricas Principais (KPI Cards)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(label="🏙️ Municípios Monitorados", value=f"{df_filtrado['municipio'].nunique()}")
    with col2:
        st.metric(label="👶 Alunos Avaliados", value=f"{df_filtrado['qtd_alunos_avaliados'].sum():,}".replace(',', '.'))
    with col3:
        prof_global = df_filtrado['proficiencia_media'].mean()
        st.metric(label="📈 Proficiência Média Global", value=f"{prof_global:.1f} pts")
    with col4:
        st.metric(label="🔋 Camada de Persistência", value="NoSQL Actived", delta="Spark Pipeline Link OK")

    st.markdown("---")

    # 📉 Distribuição Gráfica Avançada
    col_esq, col_dir = st.columns([2, 3])

    mapa_cores = {
        'Alfabetizado (≥ 743 pts)': '#2ecc71', 
        'Atenção (< 743 pts)': '#e74c3c'
    }

    with col_esq:
        st.subheader("🎯 Concentração por Status de Proficiência")

df = carregar_dados()

# --- INTERFACE ---
st.title("🌌 Dashboard Nacional: Criança Alfabetizada")
st.markdown("### Painel Executivo Intergaláctico - Consumindo Dados Gold")

if df.empty:
    st.warning("⚠️ Aguardando dados da Engenharia de Dados.")
else:
    # --- BARRA LATERAL (Filtros em caixinhas vermelhas) ---
    st.sidebar.header("🛸 Filtros Estratégicos")
    
    lista_ufs = sorted(df['sigla_uf'].dropna().unique().tolist())
    uf_selecionada = st.sidebar.multiselect("Filtrar por Estado (UF):", lista_ufs, default=[])
    
    lista_status = sorted(df['status_alfabetizacao'].dropna().unique().tolist())
    status_selecionado = st.sidebar.multiselect("Filtrar por Status Saeb:", lista_status, default=[])
    
    df_filtrado = df.copy()
    if uf_selecionada:
        df_filtrado = df_filtrado[df_filtrado['sigla_uf'].isin(uf_selecionada)]
    if status_selecionado:
        df_filtrado = df_filtrado[df_filtrado['status_alfabetizacao'].isin(status_selecionado)]

    # --- 1. METAS BRASIL E DADOS DE ALUNOS (KPIs) ---
    st.markdown("#### 1. Visão Geral (Metas Brasil & Alunos)")
    col1, col2, col3, col4 = st.columns(4)
    
    meta_brasil_media = df_filtrado['taxa_alfabetizacao'].mean()
    total_alunos = df_filtrado['qtd_alunos_avaliados'].sum()
    total_municipios = len(df_filtrado['id_municipio'].unique())
    
    col1.metric("Meta Alfabetização Brasil", f"{meta_brasil_media:.1f}%")
    col2.metric("Dados de Alunos (Avaliados)", f"{total_alunos:,.0f}".replace(',','.'))
    col3.metric("Municípios Analisados", f"{total_municipios}")
    col4.metric("Status Global", "Operacional 🟢")

    st.markdown("---")

    # --- 2. GRÁFICOS ---
    row1_col1, row1_col2 = st.columns(2)

    with row1_col1:
        st.markdown("#### 2. Meta Alfabetização por UF")
        # Agrupa por UF
        df_uf = df_filtrado.groupby('sigla_uf')['taxa_alfabetizacao'].mean().reset_index()
        fig_uf = px.bar(
            df_uf.sort_values('taxa_alfabetizacao', ascending=False), 
            x='sigla_uf', 
            y='taxa_alfabetizacao',
            color='taxa_alfabetizacao',
            color_continuous_scale='blues',
            template='plotly_dark'
        )
        fig_uf.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', bargap=0.3)
        st.plotly_chart(fig_uf, use_container_width=True)

    with row1_col2:
        st.markdown("#### 3. Município (Distribuição de Status)")
        # Conta municípios por status
        df_status = df_filtrado['status_alfabetizacao'].value_counts().reset_index()
        df_status.columns = ['status', 'contagem']
main
        fig_pie = px.pie(
            df_status, 
            names='status', 
            values='contagem',
            hole=0.4,
            color='status',
            color_discrete_map={'Meta Atingida (≥ 743 pts SAEB)': '#00f3ff', 'Atenção (< 743 pts SAEB)': '#ff003c'},
            template='plotly_dark'
        )
        fig_pie.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig_pie, use_container_width=True)

feature/reinaldo-dashboard-starwars
    with col_dir:
        st.subheader("📊 Performance de Proficiência por Município")
        fig_bar = px.bar(
            df_filtrado.sort_values(by='proficiencia_media', ascending=False), 
            x='municipio', 
            y='proficiencia_media', 
            color='status_alfabetizacao',
            color_discrete_map=mapa_cores,
            text_auto='.1f',
            template='plotly_dark',
            labels={'municipio': 'Município', 'proficiencia_media': 'Proficiência Média (SAEB)'}
        )
        
        fig_bar.add_hline(
            y=743, 
            line_dash="dash", 
            line_color="#f1c40f", 
            annotation_text="Nota de Corte Saeb (743 pts)", 
            annotation_position="top left"
        )
        fig_bar.update_layout(yaxis_range=[680, 790], margin=dict(t=20, b=20, l=10, r=10))
        st.plotly_chart(fig_bar, use_container_width=True)

# 📑 Nota de Rodapé e Alinhamento de Negócio
st.markdown("---")
st.info(
    "💡 **Análise de Negócio & Governança (CRISP-DM):** Este painel consome a tabela consolidada na camada Gold. "
    "Municípios em vermelho representam regiões prioritárias para o direcionamento de verbas do Fundo de Manutenção "
    "e Desenvolvimento da Educação Básica (Fundeb), correlacionando o desempenho técnico à infraestrutura local."
)

    st.markdown("#### 4. Meta Alfabetização por Município (Top 15 Maiores/Menores)")
    df_top_mun = df_filtrado.sort_values('taxa_alfabetizacao', ascending=False).head(15)
    fig_mun = px.bar(
        df_top_mun, 
        x='nome_municipio', 
        y='taxa_alfabetizacao',
        color='status_alfabetizacao',
        color_discrete_map={'Meta Atingida (≥ 743 pts SAEB)': '#00f3ff', 'Atenção (< 743 pts SAEB)': '#ff003c'},
        template='plotly_dark'
    )
    fig_mun.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', bargap=0.3)
    st.plotly_chart(fig_mun, use_container_width=True)
main
