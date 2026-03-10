import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import pandas as pd
import pytest
from sqlalchemy import create_engine
from load import load_data_to_sqlite

def test_load_data_to_sqlite(tmp_path):
    df = pd.DataFrame({
        "id": [1, 2],
        "nome": ["Alice", "Bob"]
    })
    
    db_file = tmp_path / "teste_banco.db"
    db_path = str(db_file)
    table_name = "test_table"
    
    # Executa a carga
    load_data_to_sqlite(df, db_path=db_path, table_name=table_name)
    
    # Verifica se os dados foram realmente persistidos no SQLite
    engine = create_engine(f"sqlite:///{db_path}")
    df_loaded = pd.read_sql(f"SELECT * FROM {table_name}", con=engine)
    engine.dispose()
    
    assert len(df_loaded) == 2
    assert list(df_loaded.columns) == ["id", "nome"]
    assert df_loaded.iloc[0]["nome"] == "Alice"

def test_load_data_to_sqlite_replace(tmp_path):
    # Cria o banco e tabela primeiro
    df_inicial = pd.DataFrame({"id": [1], "nome": ["X"]})
    df_novo = pd.DataFrame({"id": [2, 3], "nome": ["Y", "Z"]})
    
    db_file = tmp_path / "teste_banco2.db"
    db_path = str(db_file)
    table_name = "tabela_replace"
    
    # Carga inicial
    load_data_to_sqlite(df_inicial, db_path=db_path, table_name=table_name)
    
    # Carga substituindo (if_exists="replace" que é o padrão atual da implementação)
    load_data_to_sqlite(df_novo, db_path=db_path, table_name=table_name, if_exists="replace")
    
    engine = create_engine(f"sqlite:///{db_path}")
    df_loaded = pd.read_sql(f"SELECT * FROM {table_name}", con=engine)
    engine.dispose()
    
    # Se fez replace, tem 2 linhas (Y, Z)
    assert len(df_loaded) == 2
    assert "Y" in df_loaded["nome"].values
    assert "X" not in df_loaded["nome"].values
