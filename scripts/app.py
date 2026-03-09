import streamlit as st
import pandas as pd
from typing import Optional, Tuple

st.set_page_config(
    page_title="Auto-EDA Dashboard Builder",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

from extract import ExtractorFactory
from transform import transform_data
from flowchart import generate_etl_dashboard

MAX_FILE_SIZE_MB = 50


def _render_sidebar_profile(profile: dict) -> None:
    """Exibe metricas do dataset no sidebar apos o carregamento."""
    st.sidebar.divider()
    st.sidebar.markdown("### Perfil do Dataset")
    col1, col2 = st.sidebar.columns(2)
    col1.metric("Linhas",       f"{profile['total_rows']:,}")
    col2.metric("Colunas",      profile["total_cols"])
    col1.metric("Completude",   f"{profile['completeness_pct']}%")
    col2.metric("Cols. Data",   len(profile["date_cols"]))
    col1.metric("Numericas",    len(profile["numeric_cols"]))
    col2.metric("Categoricas",  len(profile["categorical_cols"]))

    null_pct = profile.get("null_pct", {})
    if null_pct:
        worst     = max(null_pct, key=null_pct.get)
        worst_val = null_pct[worst]
        if worst_val > 0:
            st.sidebar.warning(f"**{worst}** tem {worst_val}% de nulos")


def carregar_dados_memoria(
    uploaded_file,
) -> Optional[Tuple[pd.DataFrame, dict]]:
    """Extrai, transforma e devolve (DataFrame, perfil)."""
    try:
        nome_arquivo = uploaded_file.name

        if uploaded_file.size > MAX_FILE_SIZE_MB * 1024 * 1024:
            st.error(f"Arquivo muito grande. Limite: {MAX_FILE_SIZE_MB}MB.")
            return None

        st.toast(f"Detectando formato de {nome_arquivo}...", icon="⏳")
        factory   = ExtractorFactory()
        extractor = factory.get_extractor(nome_arquivo)

        df_raw              = extractor.extract(uploaded_file)
        df_transformed, profile = transform_data(df_raw)

        st.toast(
            f"{profile['total_rows']:,} registros prontos! "
            f"Completude: {profile['completeness_pct']}%",
            icon="✅",
        )
        return df_transformed, profile

    except Exception as exc:
        st.error(f"Erro ao processar o arquivo: {exc}")
        return None


def main() -> None:
    st.sidebar.title("Configuracoes")
    st.sidebar.markdown("Bem-vindo ao **Dashboard Builder** Universal.")

    menu_opcao = st.sidebar.radio(
        "Navegacao:",
        ["1. Importar Dados (Upload)", "2. Visualizar Auto-EDA"],
    )

    if "profile" in st.session_state:
        _render_sidebar_profile(st.session_state["profile"])

    st.sidebar.divider()
    st.sidebar.info(
        "Processamento 100% local — "
        "nenhum dado e enviado a servidores externos."
    )

    # ── TELA 1: UPLOAD ──────────────────────────────────────────────────────
    if menu_opcao == "1. Importar Dados (Upload)":
        st.title("Importacao de Dados Brutos")
        st.markdown(
            "Arraste sua planilha para a area abaixo. "
            "Formatos suportados: **CSV, XLSX, XLS, XML**"
        )

        upload_col, preview_col = st.columns([2, 1])

        with upload_col:
            uploaded_file = st.file_uploader(
                "Selecione ou arraste o arquivo",
                type=["csv", "xlsx", "xls", "xml"],
                help=f"Tamanho maximo: {MAX_FILE_SIZE_MB}MB",
            )

            if uploaded_file and st.button(
                "Processar na Memoria",
                use_container_width=True,
                type="primary",
            ):
                with st.spinner("Analisando e estruturando os dados..."):
                    result = carregar_dados_memoria(uploaded_file)

                if result is not None:
                    df, profile = result
                    st.session_state["df_master"] = df
                    st.session_state["profile"]   = profile
                    st.success(
                        "Tudo pronto! Navegue ate **2. Visualizar Auto-EDA** no painel lateral."
                    )

        with preview_col:
            if "df_master" in st.session_state and not st.session_state["df_master"].empty:
                profile = st.session_state["profile"]
                st.metric("Status",     "Pronto", delta="Dados em memoria")
                st.metric("Completude", f"{profile['completeness_pct']}%")
                st.dataframe(
                    st.session_state["df_master"].head(5),
                    use_container_width=True,
                )
            else:
                st.metric("Status", "Aguardando arquivo", delta_color="off")

    # ── TELA 2: DASHBOARD ────────────────────────────────────────────────────
    elif menu_opcao == "2. Visualizar Auto-EDA":
        if "df_master" not in st.session_state or st.session_state["df_master"].empty:
            st.warning(
                "Nenhum arquivo carregado. "
                "Retorne ao menu **1. Importar Dados**."
            )
            st.stop()

        profile = st.session_state.get("profile")

        fig = generate_etl_dashboard(
            df_in_memory=st.session_state["df_master"],
            profile=profile,
        )

        if fig is not None:
            st.plotly_chart(fig, use_container_width=True, theme=None)
        else:
            st.error(
                "Nao foi possivel gerar o painel. "
                "Dados vazios ou incompativeis."
            )


if __name__ == "__main__":
    main()
