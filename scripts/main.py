import argparse
import logging
import os
import sys
import uuid

from extract import extract_api_data, extract_file_data
from flowchart import generate_dashboard_from_db
from load import load_data_to_sqlite
from transform import transform_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Caminhos extraídos como constantes: facilita configuração sem alterar lógica
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_SCRIPT_DIR)
DEFAULT_CSV_PATH = os.path.join(_PROJECT_DIR, "data", "transacoes_vendas.csv")
DEFAULT_DB_PATH = os.path.join(_PROJECT_DIR, "data", "vendas.db")
DEFAULT_TABLE = "tb_vendas_convertidas"


def run_etl(
    input_file: str,
    db_path: str = DEFAULT_DB_PATH,
) -> None:
    logger.info("--- Iniciando Auto-EDA Universal ---")
    
    try:
        # [E] EXTRAÇÃO Multiformato
        logger.info("1. Extração Dinâmica...")
        df_raw = extract_file_data(input_file)

        # [T] TRANSFORMAÇÃO Genérica
        logger.info("2. Tratamento Genérico...")
        df_transformed, profile = transform_data(df_raw)

        # Usar um nome de tabela aleatório para evitar colisão entre diferentes datasets ou sobescrever fixo
        table_name = f"data_{uuid.uuid4().hex[:8]}"

        # [L] CARGA
        logger.info("3. Carga no Banco Analítico...")
        load_data_to_sqlite(df_transformed, db_path=db_path, table_name=table_name)
        logger.info("--- Dados carregados para visualização! ---")

        # [V] VISUALIZAÇÃO Auto-EDA
        logger.info("4. Visualização (Auto-EDA)...")
        generate_dashboard_from_db(db_path=db_path, table_name=table_name)

    except Exception as e:
        # Re-raise após logar: o erro sobe para o chamador e não é engolido silenciosamente
        logger.error("O pipeline falhou: %s", e, exc_info=True)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Criador Universal de Dashboards Auto-EDA")
    parser.add_argument("--arquivo", type=str, default=DEFAULT_CSV_PATH, help="Caminho do arquivo (CSV, XLSX, XML) para gerar o dashboard")
    args = parser.parse_args()

    # Verifica se o arquivo existe antes de tentar processar
    if not os.path.exists(args.arquivo):
        print("\n" + "="*60)
        print("⚠️  AVISO: ARQUIVO PADRÃO NÃO ENCONTRADO")
        print("="*60)
        print(f"O arquivo '{args.arquivo}' não existe.")
        print("\nComo o sistema foi convertido para uma aplicação Web,")
        print("recomendamos que você utilize a nova interface visual:")
        print("\n👉 Comando: streamlit run scripts/app.py")
        print("="*60 + "\n")
        sys.exit(0)

    try:
        run_etl(input_file=args.arquivo)
    except Exception:
        sys.exit(1)
