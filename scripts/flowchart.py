import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine
import os


def generate_etl_dashboard(db_path: str):
    """
    Lê os dados processados no banco SQLite e gera um dashboard interativo
    com múltiplos gráficos dos resultados do pipeline ETL.
    """

    # ── Carrega os dados do banco ────────────────────────────────────────────
    engine = create_engine(f"sqlite:///{db_path}")
    df = pd.read_sql("SELECT * FROM tb_vendas_convertidas", con=engine)
    df["data_venda"] = pd.to_datetime(df["data_venda"])

    # ── Paleta de cores ──────────────────────────────────────────────────────
    COLORS = {
        "Small":  "#4A90D9",
        "Medium": "#E07B39",
        "High":   "#9B59B6",
        "brl":    "#4CAF76",
        "usd":    "#4A90D9",
        "eur":    "#E07B39",
    }
    BG_DARK  = "#1A1A2E"
    BG_PAPER = "#16213E"
    TEXT     = "#E0E0E0"
    GRID     = "#2A2A4A"

    # ── Métricas globais ─────────────────────────────────────────────────────
    total_brl = df["valor_brl"].sum()
    total_usd = df["valor_usd"].sum()
    total_eur = df["valor_eur"].sum()
    n_trans   = len(df)
    ticket_medio = df["valor_brl"].mean()

    # ── Dados por faixa ──────────────────────────────────────────────────────
    faixa_counts  = df["faixa_valor"].value_counts().reindex(["Small", "Medium", "High"])
    faixa_revenue = df.groupby("faixa_valor")["valor_brl"].sum().reindex(["Small", "Medium", "High"])

    # ── Dados por produto ────────────────────────────────────────────────────
    prod_revenue = df.groupby("produto")["valor_brl"].sum().sort_values(ascending=True)

    # ── Evolução temporal ────────────────────────────────────────────────────
    time_series = df.groupby("data_venda")["valor_brl"].sum().reset_index()

    # ────────────────────────────────────────────────────────────────────────
    # Layout: 3 linhas × 3 colunas
    # Linha 1: 3 KPI cards (indicadores)
    # Linha 2: Rosca (faixa_valor) | Barras horizontais (produto) | Linha (tempo)
    # Linha 3: Barras agrupadas moedas | Barras contagem por faixa
    # ────────────────────────────────────────────────────────────────────────
    fig = make_subplots(
        rows=3, cols=3,
        specs=[
            [{"type": "indicator"}, {"type": "indicator"}, {"type": "indicator"}],
            [{"type": "pie"},       {"type": "bar"},        {"type": "scatter"}],
            [{"type": "bar"},       {"type": "bar"},        {"type": "bar"}],
        ],
        subplot_titles=(
            "", "", "",                                             # KPI cards sem título
            "Distribuição por Faixa de Valor",
            "Receita por Produto (BRL)",
            "Evolução das Vendas ao Longo do Tempo",
            "Receita Total por Moeda",
            "Qtd. de Transações por Faixa",
            "Ticket Médio por Faixa (BRL)",
        ),
        vertical_spacing=0.12,
        horizontal_spacing=0.08,
    )

    # ── KPI 1: Total BRL ─────────────────────────────────────────────────────
    fig.add_trace(go.Indicator(
        mode="number+delta",
        value=total_brl,
        number={"prefix": "R$ ", "valueformat": ",.2f",
                "font": {"size": 32, "color": COLORS["brl"]}},
        title={"text": f"<b>Receita Total (BRL)</b><br><span style='font-size:12px;color:{TEXT}'>{n_trans} transações</span>",
               "font": {"color": TEXT}},
        delta={"reference": ticket_medio * n_trans * 0.9, "relative": True,
               "valueformat": ".1%", "increasing": {"color": COLORS["brl"]}},
    ), row=1, col=1)

    # ── KPI 2: Total USD ─────────────────────────────────────────────────────
    fig.add_trace(go.Indicator(
        mode="number",
        value=total_usd,
        number={"prefix": "$ ", "valueformat": ",.2f",
                "font": {"size": 32, "color": COLORS["usd"]}},
        title={"text": "<b>Receita Total (USD)</b><br><span style='font-size:12px;color:#999'>Conversão via AwesomeAPI</span>",
               "font": {"color": TEXT}},
    ), row=1, col=2)

    # ── KPI 3: Total EUR ─────────────────────────────────────────────────────
    fig.add_trace(go.Indicator(
        mode="number",
        value=total_eur,
        number={"prefix": "€ ", "valueformat": ",.2f",
                "font": {"size": 32, "color": COLORS["eur"]}},
        title={"text": "<b>Receita Total (EUR)</b><br><span style='font-size:12px;color:#999'>Conversão via AwesomeAPI</span>",
               "font": {"color": TEXT}},
    ), row=1, col=3)

    # ── Gráfico 4: Pizza / Rosca — faixa_valor ───────────────────────────────
    fig.add_trace(go.Pie(
        labels=faixa_counts.index.tolist(),
        values=faixa_counts.values,
        hole=0.52,
        marker_colors=[COLORS["Small"], COLORS["Medium"], COLORS["High"]],
        textfont={"color": "white", "size": 13},
        hovertemplate="<b>%{label}</b><br>Qtd: %{value}<br>%{percent}<extra></extra>",
    ), row=2, col=1)

    # ── Gráfico 5: Barras horizontais — receita por produto ──────────────────
    fig.add_trace(go.Bar(
        y=prod_revenue.index.tolist(),
        x=prod_revenue.values,
        orientation="h",
        marker=dict(
            color=prod_revenue.values,
            colorscale=[[0, "#4A90D9"], [0.5, "#E07B39"], [1, "#9B59B6"]],
            showscale=False,
        ),
        text=[f"R$ {v:,.0f}" for v in prod_revenue.values],
        textposition="outside",
        textfont={"color": TEXT, "size": 10},
        hovertemplate="<b>%{y}</b><br>R$ %{x:,.2f}<extra></extra>",
    ), row=2, col=2)

    # ── Gráfico 6: Linha — evolução temporal ─────────────────────────────────
    fig.add_trace(go.Scatter(
        x=time_series["data_venda"],
        y=time_series["valor_brl"],
        mode="lines+markers",
        line=dict(color=COLORS["brl"], width=2.5),
        marker=dict(size=8, color=COLORS["brl"],
                    line=dict(color="white", width=1.5)),
        fill="tozeroy",
        fillcolor="rgba(76, 175, 118, 0.15)",
        hovertemplate="<b>%{x|%d/%m/%Y}</b><br>R$ %{y:,.2f}<extra></extra>",
    ), row=2, col=3)

    # ── Gráfico 7: Barras agrupadas — receita por moeda ──────────────────────
    for moeda, col_key, prefix in [("BRL", "brl", "R$"), ("USD", "usd", "$"), ("EUR", "eur", "€")]:
        col_name = f"valor_{moeda.lower()}"
        vals = [df[df["faixa_valor"] == f][col_name].sum() for f in ["Small", "Medium", "High"]]
        fig.add_trace(go.Bar(
            name=moeda,
            x=["Small", "Medium", "High"],
            y=vals,
            marker_color=COLORS[col_key],
            hovertemplate=f"<b>%{{x}}</b><br>{prefix} %{{y:,.2f}}<extra>{moeda}</extra>",
        ), row=3, col=1)

    # ── Gráfico 8: Barras — contagem por faixa ───────────────────────────────
    fig.add_trace(go.Bar(
        x=faixa_counts.index.tolist(),
        y=faixa_counts.values,
        marker_color=[COLORS["Small"], COLORS["Medium"], COLORS["High"]],
        text=faixa_counts.values,
        textposition="outside",
        textfont={"color": TEXT},
        hovertemplate="<b>%{x}</b><br>%{y} transações<extra></extra>",
        showlegend=False,
    ), row=3, col=2)

    # ── Gráfico 9: Barras — ticket médio por faixa ───────────────────────────
    ticket_faixa = df.groupby("faixa_valor")["valor_brl"].mean().reindex(["Small", "Medium", "High"])
    fig.add_trace(go.Bar(
        x=ticket_faixa.index.tolist(),
        y=ticket_faixa.values,
        marker_color=[COLORS["Small"], COLORS["Medium"], COLORS["High"]],
        text=[f"R$ {v:,.2f}" for v in ticket_faixa.values],
        textposition="outside",
        textfont={"color": TEXT},
        hovertemplate="<b>%{x}</b><br>Ticket médio: R$ %{y:,.2f}<extra></extra>",
        showlegend=False,
    ), row=3, col=3)

    # ── Estilo global ────────────────────────────────────────────────────────
    fig.update_layout(
        title=dict(
            text=(
                "<b>📊 Dashboard ETL — Resultados das Transações Financeiras</b>"
                "<br><span style='font-size:13px;color:#AAAAAA'>"
                "Pipeline: Extract → Transform → Load → View</span>"
            ),
            x=0.5, xanchor="center",
            font=dict(size=20, color="white", family="Inter, Arial, sans-serif"),
        ),
        plot_bgcolor=BG_DARK,
        paper_bgcolor=BG_PAPER,
        font=dict(color=TEXT, family="Inter, Arial, sans-serif"),
        legend=dict(
            bgcolor="rgba(255,255,255,0.05)",
            bordercolor="#444",
            borderwidth=1,
            font=dict(color=TEXT),
        ),
        barmode="group",
        height=900,
        margin=dict(l=40, r=40, t=100, b=40),
    )

    # Eixos com estilo escuro
    fig.update_xaxes(gridcolor=GRID, zerolinecolor=GRID, tickfont=dict(color=TEXT))
    fig.update_yaxes(gridcolor=GRID, zerolinecolor=GRID, tickfont=dict(color=TEXT))

    # Títulos dos subplots em branco
    for ann in fig.layout.annotations:
        ann.font.color = "#CCCCCC"
        ann.font.size  = 12

    fig.show()


if __name__ == "__main__":
    # Execução standalone — aponta para o banco gerado pelo pipeline
    script_dir  = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    db_path     = os.path.join(project_dir, "data", "vendas.db")
    generate_etl_dashboard(db_path)
