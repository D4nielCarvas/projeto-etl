# Projeto ETL - Automação e Conversão de Transações Financeiras

## 📌 Problema de Negócio
Uma empresa multinacional realiza vendas no Brasil (em BRL) mas precisa analisar o faturamento consolidado também em Dólar (USD) e Euro (EUR) para reportar à matriz. Atualmente, o processo de conversão e categorização das vendas é feito manualmente em planilhas, o que gera erros, dados duplicados e lentidão.

O objetivo deste projeto é construir um Pipeline de ETL (Extract, Transform, Load) em Python que:
- Extraia cotações de moedas em tempo real via API.
- Faça a leitura das transações de vendas de um sistema de origem (simulado via CSV).
- Limpe os dados (tratando valores nulos e possíveis duplicatas originadas no sistema).
- Aplique regras de negócio para conversão de moedas e categorização do ticket de vendas.
- Salve os dados processados e confiáveis num banco de dados relacional para consumo de painéis e relatórios.
- Gere um **dashboard interativo** com gráficos dos resultados reais da execução.

## 🛠️ Tecnologias Utilizadas
- **Linguagem:** Python 3
- **Extração de Dados:** `requests` (consumo de API RESTful) e `pandas` (leitura de arquivos)
- **Transformação (Manipulação de Dados):** `pandas` e `numpy`
- **Carga (Banco de Dados):** SQLite e `sqlalchemy` (ORM / Engine para persistência)
- **Visualização:** `plotly` (dashboard interativo com KPIs e gráficos dos resultados)

## 🏗️ Estrutura do Projeto (Modularização)
- `scripts/extract.py`: Módulo responsável pela leitura do arquivo CSV e chamada da 'AwesomeAPI' de moedas.
- `scripts/transform.py`: Módulo que concentra as regras de negócio, limpeza (remoção de nulos/duplicatas), cálculos de conversão e criação da coluna categórica de faixas de valor.
- `scripts/load.py`: Módulo que gerencia a conexão com o banco de dados e persistência dos dados transformados usando SQLAlchemy.
- `scripts/flowchart.py`: Módulo que lê os dados do SQLite e gera um **dashboard interativo** com Plotly, exibindo KPIs e gráficos dos resultados do pipeline.
- `scripts/main.py`: Orquestrador do pipeline, responsável por integrar e executar o fluxo ETL sequencialmente e chamar o dashboard ao final.
- `data/`: Diretório onde ficam localizados o arquivo de entrada `.csv` e o banco de dados de saída `.db`.

## 📊 Dashboard de Resultados (etapa View)

Ao final de cada execução, é gerado automaticamente um **dashboard interativo** no navegador com:

| Visualização | Descrição |
|---|---|
| 💰 KPI — Receita Total BRL | Total faturado em reais |
| 💵 KPI — Receita Total USD | Total convertido para dólar |
| 💶 KPI — Receita Total EUR | Total convertido para euro |
| 🍩 Distribuição por Faixa | Proporção de transações Small / Medium / High |
| 📊 Receita por Produto | Faturamento individual de cada produto (BRL) |
| 📅 Evolução Temporal | Vendas ao longo do período analisado |
| 🌍 Receita por Moeda/Faixa | Comparativo BRL × USD × EUR por categoria |
| 🔢 Qtd. por Faixa | Contagem de transações por categoria |
| 🎫 Ticket Médio por Faixa | Valor médio de venda por categoria |

> O dashboard também pode ser gerado isoladamente após rodar o pipeline: `python scripts/flowchart.py`

## 🚀 Como Executar o Projeto

1. **Clone o repositório e acesse a pasta do projeto.**

2. **Crie um ambiente virtual (opcional, mas recomendado):**
   ```bash
   python -m venv venv
   # Ative no Windows:
   venv\Scripts\activate
   # Ative no Linux/Mac:
   source venv/bin/activate
   ```

3. **Instale as dependências:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Execute o pipeline integrado:**
   ```bash
   python scripts/main.py
   ```

Após a execução:
- Um arquivo de banco de dados (`vendas.db`) será criado na pasta `data/` com a tabela `tb_vendas_convertidas`.
- O dashboard interativo será aberto automaticamente no navegador com os resultados da execução.
