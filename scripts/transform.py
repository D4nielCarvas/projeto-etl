import pandas as pd

def transform_data(df: pd.DataFrame, cotacoes: dict) -> pd.DataFrame:
    
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
    
    # 4. Criando coluna categórica (faixa_valor) com pd.cut + thresholds dinâmicos
    # Os limites são calculados automaticamente via percentis (p33 e p66) do próprio dataset
    p33 = df_transformed['valor_brl'].quantile(0.33)
    p66 = df_transformed['valor_brl'].quantile(0.66)

    df_transformed['faixa_valor'] = pd.cut(
        df_transformed['valor_brl'],
        bins=[0, p33, p66, float('inf')],
        labels=['Small', 'Medium', 'High']
    )
    
    return df_transformed