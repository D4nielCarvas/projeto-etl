# Auto-EDA Universal 📊 — Pipeline de ETL & Dashboard Financeiro

## 📌 Visão Geral
Este projeto é um ecossistema de ETL (Extract, Transform, Load) projetado para automatizar a consolidação de faturamento e gerar dashboards de **Auto-EDA** (Exploratory Data Analysis) de forma instantânea. 

Originalmente focado em conversão de moedas (BRL para USD/EUR), o sistema evoluiu para um **Analista de Dados Universal** que processa múltiplos formatos e gera visualizações inteligentes em memória.

## 🚀 Como Executar o Sistema (Interface Web)

Esta é a forma recomendada de utilize o projeto. Permite upload de arquivos e visualização dinâmica.

1.  **Prepare o Ambiente:**
    ```powershell
    python -m venv .venv
    .\.venv\Scripts\activate  # Windows
    # No Linux/Mac: source .venv/bin/activate
    
    pip install -r requirements.txt
    ```

2.  **Inicie a Aplicação:**
    ```powershell
    streamlit run scripts/app.py
    ```
    *Acesse a URL gerada (ex: http://localhost:8501) para subir seus dados (CSV, XLSX ou XML).*

---

## 🏗️ Arquitetura e Estrutura (Modular)

O projeto segue princípios de **Clean Code** e **SOLID**, garantindo que cada componente tenha uma única responsabilidade:

-   **`scripts/app.py`**: Interface visual principal (Streamlit). Gerencia o fluxo de upload e visualização em memória.
-   **`scripts/extract.py`**: Utiliza o Design Pattern **Factory** para extração dinâmica baseada na extensão do arquivo.
-   **`scripts/transform.py`**: Camada de lógica de negócio — limpeza, tratamento de tipos, conversão de moedas via API e enriquecimento de dados.
-   **`scripts/load.py`**: Persistência em banco de dados relacional via SQLAlchemy/SQLite (utilizado no modo CLI).
-   **`scripts/flowchart.py`**: Motor de visualização que gera dashboards interativos usando **Plotly**.
-   **`scripts/main.py`**: Orquestrador legitmo para processamento em lote via linha de comando.

---

## 🛠️ Tecnologias Principais
-   **Data Stack:** `pandas`, `numpy`, `pandera` (validação de dados).
-   **Web/UI:** `streamlit`, `plotly`.
-   **DB/ORM:** `SQLAlchemy`, `SQLite`.
-   **Extração:** `requests` (AwesomeAPI), `openpyxl`, `lxml`.

## 📊 Dashboard de Resultados
Ao carregar seus dados, o sistema gera automaticamente KPIs e gráficos de:
-   **Perfíl do Dataset:** Completude, tipos de dados e volumetria.
-   **Distribuições:** Recetas por categoria, ticket médio e distribuição por faixa de valor.
-   **Evolução:** Séries temporais e performance por produto.

---

## ⚙️ Interface de Linha de Comando (Modo CLI)
Caso deseje processar um arquivo local salvando os resultados em banco de dados:
```bash
python scripts/main.py --arquivo data/transacoes_vendas.csv
```
*Os resultados serão persistidos em `data/vendas.db`.*
