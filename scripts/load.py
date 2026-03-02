import pandas as pd
from sqlalchemy import create_engine

def load_data_to_sqlite(df: pd.DataFrame, db_path: str, table_name: str):
    """
    Carrega o DataFrame final no banco de dados SQLite local utilizando SQLAlchemy.
    Se a tabela já existir, ela será substituída.
    """
    # Criando o motor do banco de dados (engine). 
    # Em sqlite o caminho usa sqlite:///caminho_absoluto_ou_relativo
    engine = create_engine(f'sqlite:///{db_path}')
    
    # Salvando os dados na tabela SQLite
    # if_exists='replace' substitui a tabela inteira a cada execução
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    
    print(f"[{len(df)} linhas] carregadas com sucesso na tabela '{table_name}' no banco SQLite.")
