# Auto-EDA Universal 📊 — Pipeline de ETL & Dashboard Financeiro

## 📌 Visão Geral
Este projeto é um ecossistema de ETL (Extract, Transform, Load) que automatiza a análise e visualização de dados tabulares. O sistema processa múltiplos formatos de arquivo (CSV, XLSX, XLS, XML) e gera dashboards de **Auto-EDA** (Exploratory Data Analysis) de forma automática.

---

## 🚀 Como Executar (Interface Web — Recomendado)

### 1. Prepare o Ambiente

```powershell
python -m venv .venv
.\.venv\Scripts\activate

pip install -r requirements.txt
```

### 2. Inicie a Aplicação

```powershell
streamlit run scripts/app.py
```

Acesse `http://localhost:8501`, faça upload de qualquer planilha e o dashboard será gerado automaticamente.

> **📂 Arquivo de exemplo:** `data/exemplo.csv` — pronto para testar sem precisar de seus próprios dados.

---

## 🏗️ Arquitetura Modular

O projeto segue os princípios de **Clean Code** e **SOLID**, com responsabilidade única por módulo:

| Arquivo | Responsabilidade | Design Pattern |
|---|---|---|
| `scripts/app.py` | Interface visual (Streamlit), upload e navegação | — |
| `scripts/extract.py` | Extração multiformato (CSV, XLSX, XLS, XML) | **Strategy + Factory** |
| `scripts/transform.py` | Limpeza, normalização, inferência de tipos | — |
| `scripts/load.py` | Persistência SQLite (modo CLI) | — |
| `scripts/flowchart.py` | Geração de dashboard interativo com Plotly | — |
| `scripts/main.py` | Orquestrador para processamento em lote via CLI | — |

---

## 🛠️ Tecnologias

| Camada | Libs |
|---|---|
| Data | `pandas`, `numpy` |
| Web/UI | `streamlit`, `plotly` |
| DB/ORM | `SQLAlchemy`, `SQLite` |
| Leitura | `openpyxl` (xlsx), `lxml` (xml) |
| Testes | `pytest` |

---

## 📊 Dashboard Auto-EDA

Ao carregar seus dados, o sistema gera automaticamente:

- **KPIs** — total de registros, completude e soma da coluna principal
- **Qualidade dos Dados** — completude por coluna (verde/amarelo/vermelho)
- **Distribuição** — gráfico de rosca pela categoria principal
- **Ranking** — Top 10 categorias por valor
- **Série Temporal** — evolução ao longo do tempo (se houver coluna de data)
- **Violin Plot** — distribuição por categoria
- **Heatmap de Correlação** — entrelaçamento entre variáveis numéricas

---

## ⚙️ Modo CLI (Linha de Comando)

Para processar um arquivo local e persistir o resultado em SQLite:

```powershell
# Da raiz do projeto
python scripts/main.py --arquivo data/exemplo.csv
```

Os resultados serão salvos em `data/vendas.db`.

---

## 🧪 Testes Automatizados

```powershell
pytest tests/ -v
```

Os testes cobrem as principais funções de transformação: normalização de colunas, parsing de números BR, detecção de datas e limpeza genérica de DataFrames.
