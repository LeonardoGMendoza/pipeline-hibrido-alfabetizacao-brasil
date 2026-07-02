from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = ROOT / "data" / "gold" / "indicador_alfabetizacao.parquet"

st.set_page_config(
    page_title="Alfabetiza Brasil Analytics",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .stApp {
        background: linear-gradient(135deg, #07111f 0%, #0f1f3a 100%);
        color: #f8fafc;
    }
    div[data-testid="stMetric"] {
        background-color: rgba(15, 23, 42, 0.85);
        border: 1px solid rgba(148, 163, 184, 0.35);
        border-radius: 10px;
        padding: 12px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.database.aws_s3_connector import carregar_camada_gold_s3

@st.cache_data(ttl=600)
def carregar_dados():
    try:
        # Se tivermos os dados temporais (simulação), carrega eles!
        caminho_temporal = ROOT / "data" / "gold" / "indicador_alfabetizacao_temporal.parquet"
        if caminho_temporal.exists():
            df = pd.read_parquet(caminho_temporal)
        else:
            # O conector tenta ler da AWS S3, se falhar, lê do arquivo local (fallback automático)
            df = carregar_camada_gold_s3()
        
        if df is None or df.empty:
            return pd.DataFrame()
        for col in ["taxa_alfabetizacao", "proficiencia_media", "qtd_alunos_avaliados"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "status_alfabetizacao" not in df.columns:
            df["status_alfabetizacao"] = "Não informado"
        if "vulnerabilidade_social" not in df.columns:
            df["vulnerabilidade_social"] = "Não informado"
        if "nome_municipio" not in df.columns:
            df["nome_municipio"] = df.get("municipio", pd.Series(["Não informado"] * len(df)))
        if "sigla_uf" not in df.columns:
            df["sigla_uf"] = "BR"
        if "id_municipio" not in df.columns:
            df["id_municipio"] = range(len(df))

        return df
    except Exception as exc:
        st.warning(f"Não foi possível ler os dados em {DATA_PATH}: {exc}")
        return pd.DataFrame()


df = carregar_dados()

st.title("🌌 Dashboard Nacional: Criança Alfabetizada")
st.caption("Painel executivo com dados Gold prontos para análise e visualização")
st.markdown("---")

st.sidebar.header("🛸 Filtros Estratégicos")
st.sidebar.markdown("Aplique filtros para explorar o cenário analítico.")

if df.empty:
    st.info(
        "Ainda não há dados Gold disponíveis. Gere o parquet em data/gold ou execute a pipeline para popular este painel."
    )
else:
    # Filtro de Ano (Simulação de Streaming/Batch)
    ano_selecionado = None
    if "ano" in df.columns:
        lista_anos = sorted(df["ano"].dropna().unique().tolist(), reverse=True)
        ano_selecionado = st.sidebar.selectbox("📅 Selecione o Ano (Simulação):", lista_anos, index=0)
        st.sidebar.markdown("*(2023-2025 = Histórico Batch | 2026 = Novo Streaming)*")

    lista_ufs = sorted(df["sigla_uf"].dropna().astype(str).unique().tolist())
    uf_selecionada = st.sidebar.multiselect("Filtrar por Estado (UF):", lista_ufs, default=lista_ufs)

    lista_status = sorted(df["status_alfabetizacao"].dropna().astype(str).unique().tolist())
    status_selecionado = st.sidebar.multiselect("Filtrar por Status Saeb:", lista_status, default=lista_status)

    df_filtrado = df.copy()
    if ano_selecionado:
        df_filtrado = df_filtrado[df_filtrado["ano"] == ano_selecionado]
    if uf_selecionada:
        df_filtrado = df_filtrado[df_filtrado["sigla_uf"].astype(str).isin(uf_selecionada)]
    if status_selecionado:
        df_filtrado = df_filtrado[df_filtrado["status_alfabetizacao"].astype(str).isin(status_selecionado)]

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Municípios analisados", f"{df_filtrado['id_municipio'].nunique()}")
    with col2:
        total_alunos = int(df_filtrado["qtd_alunos_avaliados"].sum()) if "qtd_alunos_avaliados" in df_filtrado.columns else 0
        st.metric("Alunos avaliados", f"{total_alunos:,}".replace(",", "."))
    with col3:
        meta_brasil_media = float(df_filtrado["taxa_alfabetizacao"].mean()) if "taxa_alfabetizacao" in df_filtrado.columns else 0.0
        st.metric("Meta alfabetização", f"{meta_brasil_media:.1f}%")
    with col4:
        st.metric("Status global", "Operacional 🟢")

    st.markdown("---")

    col_esq, col_dir = st.columns([2, 3])
    with col_esq:
        st.subheader("Meta alfabetização por UF")
        df_uf = df_filtrado.groupby("sigla_uf")["taxa_alfabetizacao"].mean().reset_index()
        fig_uf = px.bar(
            df_uf.sort_values("taxa_alfabetizacao", ascending=False),
            x="sigla_uf",
            y="taxa_alfabetizacao",
            color="taxa_alfabetizacao",
            color_continuous_scale="blues",
            template="plotly_dark",
        )
        fig_uf.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_uf, use_container_width=True)

    with col_dir:
        st.subheader("Distribuição por status")
        df_status = df_filtrado["status_alfabetizacao"].value_counts().reset_index()
        df_status.columns = ["status", "contagem"]
        fig_pie = px.pie(
            df_status,
            names="status",
            values="contagem",
            hole=0.4,
            template="plotly_dark",
        )
        fig_pie.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_pie, use_container_width=True)

    st.markdown("---")
    st.subheader("Performance por município")
    if "proficiencia_media" in df_filtrado.columns:
        fig_bar = px.bar(
            df_filtrado.sort_values("proficiencia_media", ascending=False).head(15),
            x="nome_municipio",
            y="proficiencia_media",
            color="status_alfabetizacao",
            template="plotly_dark",
        )
        fig_bar.add_hline(y=743, line_dash="dash", line_color="#f1c40f", annotation_text="Nota de corte SAEB")
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("A coluna de proficiência média ainda não está disponível no dataset Gold.")
