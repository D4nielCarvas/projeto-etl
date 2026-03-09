import logging
import os
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

# ── Tema Visual Premium ─────────────────────────────────────────────────────
BG_DARK  = "#0d1117"
BG_PAPER = "#0d1117"
BG_CARD  = "#161b22"
TEXT     = "#e6edf3"
SUBTEXT  = "#8b949e"
GRID     = "#21262d"
SUCCESS  = "#3fb950"
WARNING  = "#d29922"
DANGER   = "#f85149"

PALETTE = [
    "#58a6ff", "#3fb950", "#d29922", "#bc8cff",
    "#f78166", "#39d353", "#ff9a3c", "#56d364",
]


# ── Helpers ─────────────────────────────────────────────────────────────────
def _load_generic_dataframe(db_path: str, table_name: str) -> pd.DataFrame:
    if not os.path.exists(db_path):
        return pd.DataFrame()
    engine = create_engine(f"sqlite:///{db_path}")
    try:
        return pd.read_sql(f'SELECT * FROM "{table_name}"', con=engine)  # noqa: S608
    finally:
        engine.dispose()


def _pick_best_categorical(df: pd.DataFrame, max_unique: int = 25) -> Optional[str]:
    """Retorna a coluna categorica com melhor cardinalidade para visualizacao."""
    candidates = df.select_dtypes(include=["object", "category"]).columns
    best, best_n = None, 0
    for col in candidates:
        n = df[col].nunique()
        if 2 <= n <= max_unique and n > best_n:
            best, best_n = col, n
    return best


def _completeness_color(value: float) -> str:
    if value >= 90:
        return SUCCESS
    if value >= 60:
        return WARNING
    return DANGER


def _col_label(col: str) -> str:
    return col.replace("_", " ").title()


# ── Secao 1: KPIs ────────────────────────────────────────────────────────────
def _build_kpis(fig: go.Figure, df: pd.DataFrame, profile: dict) -> None:
    numerics     = profile.get("numeric_cols", [])
    completeness = profile.get("completeness_pct", 100.0)

    kpi_3_val   = len(numerics)
    kpi_3_label = "Features Numericas<br><sub>Colunas detectadas</sub>"

    if numerics:
        best_num    = numerics[0]
        kpi_3_val   = df[best_num].sum()
        kpi_3_label = f"Soma — {_col_label(best_num)}<br><sub>Coluna principal</sub>"

    kpis = [
        (profile["total_rows"], PALETTE[0], "Total de Registros<br><sub>Linhas no DataSet</sub>"),
        (completeness,          _completeness_color(completeness),
         f"Completude Geral<br><sub>{completeness}% de celulas preenchidas</sub>"),
        (kpi_3_val,             PALETTE[2], kpi_3_label),
    ]

    for col_idx, (value, color, title) in enumerate(kpis, start=1):
        if isinstance(value, float) and abs(value) < 1000:
            val_fmt = ".1f"
        elif isinstance(value, (int, float)) and abs(value) >= 1000:
            val_fmt = ",.0f"
        else:
            val_fmt = ""

        fig.add_trace(
            go.Indicator(
                mode="number",
                value=float(value),
                number={"valueformat": val_fmt,
                        "font": {"size": 42, "color": color, "family": "Inter, Arial"}},
                title={"text": title,
                       "font": {"color": SUBTEXT, "size": 12, "family": "Inter, Arial"}},
            ),
            row=1, col=col_idx,
        )


# ── Secao 2: Qualidade de Dados (full-width) ─────────────────────────────────
def _build_data_quality_bar(fig: go.Figure, df: pd.DataFrame) -> None:
    completeness_by_col = ((1 - df.isnull().sum() / max(len(df), 1)) * 100).round(1)
    completeness_by_col = completeness_by_col.sort_values(ascending=True)

    bar_colors = [_completeness_color(v) for v in completeness_by_col.values]
    labels     = [f"{v:.1f}%" for v in completeness_by_col.values]

    fig.add_trace(
        go.Bar(
            y=completeness_by_col.index.tolist(),
            x=completeness_by_col.values,
            orientation="h",
            marker=dict(color=bar_colors, line=dict(color=BG_DARK, width=0.5)),
            text=labels,
            textposition="outside",
            textfont=dict(color=TEXT, size=10),
            hovertemplate="<b>%{y}</b><br>Completude: %{x:.1f}%<extra></extra>",
            cliponaxis=False,
        ),
        row=2, col=1,
    )
    fig.update_xaxes(range=[0, 115], row=2, col=1, showticklabels=False)
    fig.update_yaxes(tickfont=dict(size=10, color=SUBTEXT), row=2, col=1)


# ── Secao 3 & 4: Graficos Analiticos ────────────────────────────────────────
def _build_charts(fig: go.Figure, df: pd.DataFrame, profile: dict) -> None:
    numerics     = profile.get("numeric_cols", [])
    categoricals = profile.get("categorical_cols", [])
    dates        = profile.get("date_cols", [])
    best_cat     = _pick_best_categorical(df)

    # ── Grafico 1 (row=3, col=1): Rosca – distribuicao proporcional ──
    pie_col = best_cat or (categoricals[0] if categoricals else None)
    if pie_col:
        cat_counts = df[pie_col].value_counts().head(7)
        fig.add_trace(
            go.Pie(
                labels=cat_counts.index.tolist(),
                values=cat_counts.values,
                hole=0.52,
                marker=dict(colors=PALETTE, line=dict(color=BG_DARK, width=2)),
                textfont={"color": TEXT, "size": 11},
                hovertemplate="<b>%{label}</b><br>Qtd: %{value:,}<br>%{percent}<extra></extra>",
                name=pie_col,
            ),
            row=3, col=1,
        )
        fig.layout.annotations[3].update(text=f"Proporcao — {_col_label(pie_col)}")

    # ── Grafico 2 (row=3, col=2): Barras horizontais com gradiente ──
    if best_cat and numerics:
        num_col = numerics[0]
        agg     = df.groupby(best_cat)[num_col].sum().sort_values(ascending=True).tail(10)
        fig.add_trace(
            go.Bar(
                y=agg.index.tolist(),
                x=agg.values,
                orientation="h",
                marker=dict(
                    color=agg.values,
                    colorscale=[[0, PALETTE[0]], [0.5, PALETTE[3]], [1, PALETTE[1]]],
                    showscale=False,
                ),
                hovertemplate="<b>%{y}</b><br>Valor: %{x:,.2f}<extra></extra>",
            ),
            row=3, col=2,
        )
        fig.layout.annotations[4].update(
            text=f"Top 10 — {_col_label(num_col)} por {_col_label(best_cat)}"
        )
    elif len(numerics) >= 2:
        fig.add_trace(
            go.Scatter(
                x=df[numerics[0]], y=df[numerics[1]],
                mode="markers",
                marker=dict(color=PALETTE[0], size=5, opacity=0.6),
                hovertemplate=f"{numerics[0]}: %{{x}}<br>{numerics[1]}: %{{y}}<extra></extra>",
            ),
            row=3, col=2,
        )
        fig.layout.annotations[4].update(
            text=f"Dispersao — {_col_label(numerics[0])} x {_col_label(numerics[1])}"
        )

    # ── Grafico 3 (row=3, col=3): Serie temporal ou Histograma ──
    if dates and numerics:
        date_col = dates[0]
        num_col  = numerics[0]
        ts = df.groupby(date_col)[num_col].sum().reset_index().sort_values(date_col)
        fig.add_trace(
            go.Scatter(
                x=ts[date_col], y=ts[num_col],
                mode="lines",
                line=dict(color=PALETTE[3], width=2),
                fill="tozeroy",
                fillcolor="rgba(188, 140, 255, 0.10)",
                hovertemplate="%{x|%d/%m/%Y}<br>%{y:,.2f}<extra></extra>",
            ),
            row=3, col=3,
        )
        fig.layout.annotations[5].update(text=f"Serie Temporal — {_col_label(num_col)}")
        fig.update_xaxes(
            rangeslider=dict(visible=True, thickness=0.07,
                             bgcolor="rgba(255,255,255,0.04)"),
            row=3, col=3,
        )
    elif numerics:
        num_col = numerics[0]
        fig.add_trace(
            go.Histogram(
                x=df[num_col],
                marker=dict(color=PALETTE[3], line=dict(color=BG_DARK, width=0.5)),
                hovertemplate="Faixa: %{x}<br>Frequencia: %{y}<extra></extra>",
            ),
            row=3, col=3,
        )
        fig.layout.annotations[5].update(text=f"Distribuicao — {_col_label(num_col)}")

    # ── Grafico 4 (row=4, col=1): Count bar segunda categorica ──
    cat2       = [c for c in categoricals if c != (best_cat or "")]
    cat_to_use = (cat2[0] if cat2 else best_cat) or (categoricals[0] if categoricals else None)
    if cat_to_use:
        counts = df[cat_to_use].value_counts().head(8)
        fig.add_trace(
            go.Bar(
                x=counts.index.tolist(),
                y=counts.values,
                marker=dict(color=list(range(len(counts))),
                            colorscale="Teal", showscale=False),
                hovertemplate="<b>%{x}</b><br>Contagem: %{y:,}<extra></extra>",
            ),
            row=4, col=1,
        )
        fig.layout.annotations[6].update(text=f"Contagem — {_col_label(cat_to_use)}")

    # ── Grafico 5 (row=4, col=2): Violin plot ──
    if numerics and best_cat:
        num_col  = numerics[0]
        top_cats = df[best_cat].value_counts().head(6).index.tolist()
        for i, cat_val in enumerate(top_cats):
            subset = df[df[best_cat] == cat_val][num_col].dropna()
            color  = PALETTE[i % len(PALETTE)]
            r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
            fig.add_trace(
                go.Violin(
                    y=subset,
                    name=str(cat_val),
                    box_visible=True,
                    meanline_visible=True,
                    line_color=color,
                    fillcolor=f"rgba({r},{g},{b},0.15)",
                    opacity=0.9,
                    hovertemplate=f"{best_cat}: {cat_val}<br>%{{y:.2f}}<extra></extra>",
                ),
                row=4, col=2,
            )
        fig.layout.annotations[7].update(
            text=f"Violin — {_col_label(num_col)} por {_col_label(best_cat)}"
        )

    # ── Grafico 6 (row=4, col=3): Heatmap de correlacao ──
    if len(numerics) >= 2:
        corr = df[numerics].corr()
        fig.add_trace(
            go.Heatmap(
                z=corr.values,
                x=[_col_label(c) for c in corr.columns],
                y=[_col_label(c) for c in corr.index],
                colorscale="RdBu",
                zmid=0,
                text=[[f"{v:.2f}" for v in row] for row in corr.values],
                texttemplate="%{text}",
                textfont=dict(size=10),
                hovertemplate="<b>%{y} x %{x}</b><br>Correlacao: %{z:.2f}<extra></extra>",
                showscale=True,
                colorbar=dict(
                    thickness=12, outlinewidth=0,
                    tickfont=dict(color=SUBTEXT, size=9),
                ),
            ),
            row=4, col=3,
        )
        fig.layout.annotations[8].update(text="Matriz de Correlacao")


# ── Funcao Principal ─────────────────────────────────────────────────────────
def generate_etl_dashboard(
    db_path: str = None,
    table_name: str = "generic_data",
    df_in_memory: pd.DataFrame = None,
    profile: dict = None,
) -> Optional[go.Figure]:
    """Gera dashboard Auto-EDA premium. Aceita DataFrame direto ou SQLite."""
    if df_in_memory is not None:
        df = df_in_memory
    elif db_path:
        df = _load_generic_dataframe(db_path, table_name)
    else:
        return None

    if df.empty:
        logger.warning("Nenhum dado para gerar dashboard.")
        return None

    if profile is None:
        from transform import profile_dataframe
        profile = profile_dataframe(df)

    fig = make_subplots(
        rows=4, cols=3,
        row_heights=[0.09, 0.12, 0.42, 0.37],
        specs=[
            [{"type": "indicator"}, {"type": "indicator"}, {"type": "indicator"}],
            [{"type": "xy", "colspan": 3}, None, None],
            [{"type": "domain"},            {"type": "xy"}, {"type": "xy"}],
            [{"type": "xy"},                {"type": "xy"}, {"type": "xy"}],
        ],
        subplot_titles=(
            "", "", "",
            "Qualidade dos Dados — Completude por Coluna",
            "Painel A", "Painel B", "Painel C",
            "Painel D", "Painel E", "Painel F",
        ),
        vertical_spacing=0.07,
        horizontal_spacing=0.07,
    )

    _build_kpis(fig, df, profile)

    try:
        _build_data_quality_bar(fig, df)
    except Exception as exc:
        logger.warning("Falha no painel de qualidade: %s", exc)

    try:
        _build_charts(fig, df, profile)
    except Exception as exc:
        logger.warning("Falha ao gerar graficos analiticos: %s", exc)

    n_rows   = profile["total_rows"]
    n_cols   = profile["total_cols"]
    comp     = profile["completeness_pct"]
    subtitle = f"<sup>  {n_rows:,} registros · {n_cols} colunas · {comp}% de completude</sup>"

    fig.update_layout(
        title=dict(
            text=f"<b>Auto-EDA Dashboard</b>{subtitle}",
            x=0.5, xanchor="center",
            font=dict(size=20, color=TEXT, family="Inter, Arial, sans-serif"),
        ),
        plot_bgcolor=BG_DARK,
        paper_bgcolor=BG_PAPER,
        font=dict(color=TEXT, family="Inter, Arial, sans-serif"),
        showlegend=False,
        height=1050,
        margin=dict(l=60, r=60, t=90, b=50),
        hoverlabel=dict(
            bgcolor=BG_CARD,
            bordercolor=GRID,
            font=dict(color=TEXT, size=12),
        ),
    )

    fig.update_xaxes(gridcolor=GRID, zerolinecolor=GRID,
                     tickfont=dict(color=SUBTEXT, size=10))
    fig.update_yaxes(gridcolor=GRID, zerolinecolor=GRID,
                     tickfont=dict(color=SUBTEXT, size=10))

    for ann in fig.layout.annotations:
        ann.font.update(color=TEXT, size=12, family="Inter, Arial, sans-serif")

    logger.info("Dashboard Auto-EDA premium gerado com sucesso.")
    return fig
