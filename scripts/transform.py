import pandas as pd
import numpy as np

def transform_data(df: pd.DataFrame, cotacoes: dict) -> pd.DataFrame:
    """
    Realiza a limpeza e transformação dos dados:
    1. Remove duplicatas.
    2. Trata valores nulos (remove linhas sem valor da transação).
    3. Converte valores de BRL para USD e EUR com base nas cotações.
    4. Cria a coluna categórica 'faixa_valor' (Small, Medium, High).
    """
    # 1. Removendo duplicatas baseadas em todas as colunas
    df_transformed = df.drop_duplicates().copy()
    
    # 2. Tratando valores nulos na coluna essencial (valor_brl)
    df_transformed = df_transformed.dropna(subset=['valor_brl'])
    
    # Resetando index para ter uma tabela mais limpa
    df_transformed.reset_index(drop=True, inplace=True)
    
    # 3. Conversão de moedas
    usd_rate = cotacoes.get('USD')
    eur_rate = cotacoes.get('EUR')
    
    # Divisão pelo valor da cotação para achar o equivalente na moeda estrangeira (Cotação é BRL para 1 USD)
    df_transformed['valor_usd'] = round(df_transformed['valor_brl'] / usd_rate, 2)
    df_transformed['valor_eur'] = round(df_transformed['valor_brl'] / eur_rate, 2)
    
    # 4. Criando coluna categórica (faixa_valor) com np.select
    # Regra de negócio estipulada: < 200: Small | >= 200 e <= 1000: Medium | > 1000: High
    condicoes = [
        (df_transformed['valor_brl'] < 200),
        (df_transformed['valor_brl'] >= 200) & (df_transformed['valor_brl'] <= 1000),
        (df_transformed['valor_brl'] > 1000)
    ]
    valores = ['Small', 'Medium', 'High']
    df_transformed['faixa_valor'] = np.select(condicoes, valores, default='Unknown')
    
    return df_transformed
