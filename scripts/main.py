from extract import extract_api_data, extract_csv_data
from transform import transform_data
from load import load_data_to_sqlite
from flowchart import generate_etl_dashboard
import os

def run_etl():
    print("--- Iniciando o processo ETL ---")

    script_dir  = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)

    csv_path = os.path.join(project_dir, 'data', 'transacoes_vendas.csv')
    db_path  = os.path.join(project_dir, 'data', 'vendas.db')

    try:
        # [E] EXTRACAO
        print("\n1. Extração (Extract)...")
        cotacoes = extract_api_data()
        print(f"Cotações Obtidas (BRL): Dólar = R${cotacoes['USD']:.2f} | Euro = R${cotacoes['EUR']:.2f}")

        df_raw = extract_csv_data(csv_path)
        print(f"Lidas {len(df_raw)} transações do CSV bruto.")

        # [T] TRANSFORMACAO
        print("\n2. Transformação (Transform)...")
        df_transformed = transform_data(df_raw, cotacoes)
        print(f"Dados limpos: {len(df_transformed)} registros após remoção de duplicatas/nulos.")
        print("Amostra dos dados processados:")
        print(df_transformed.head(3))

        # [L] CARGA
        print("\n3. Carga (Load)...")
        load_data_to_sqlite(df_transformed, db_path=db_path, table_name='tb_vendas_convertidas')

        print("\n--- Processo ETL finalizado com SUCESSO! ---")

    except Exception as e:
        print(f"\n[ERRO] O pipeline falhou: {e}")
        return

    # [V] VISUALIZACAO
    print("\n4. Visualização (View) — Gerando dashboard de resultados...")
    generate_etl_dashboard(db_path=db_path)

if __name__ == "__main__":
    run_etl()
