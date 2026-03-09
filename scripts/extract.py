import logging
import os
from abc import ABC, abstractmethod
from typing import Dict, Any

import pandas as pd

logger = logging.getLogger(__name__)

# ── Padrão Strategy para Extração ─────────────────────────────────────────────
class FileExtractor(ABC):
    """Interface base para extratores de arquivos."""
    @abstractmethod
    def extract(self, filepath: str) -> pd.DataFrame:
        pass


class CSVExtractor(FileExtractor):
    def extract(self, filepath: str) -> pd.DataFrame:
        df = pd.read_csv(filepath)
        logger.info("CSV carregado: %d linhas extraídas.", len(df))
        return df


class ExcelExtractor(FileExtractor):
    def extract(self, filepath: str) -> pd.DataFrame:
        df = pd.read_excel(filepath)
        logger.info("Excel carregado: %d linhas extraídas.", len(df))
        return df


class XMLExtractor(FileExtractor):
    def extract(self, filepath: str) -> pd.DataFrame:
        df = pd.read_xml(filepath)
        logger.info("XML carregado: %d linhas extraídas.", len(df))
        return df


class ExtractorFactory:
    """Fábrica que decide qual extrator usar com base na extensão."""
    def __init__(self):
        self._extractors = {
            ".csv": CSVExtractor(),
            ".xlsx": ExcelExtractor(),
            ".xls": ExcelExtractor(),
            ".xml": XMLExtractor(),
        }

    def get_extractor(self, filepath: str) -> FileExtractor:
        _, ext = os.path.splitext(filepath)
        ext = ext.lower()
        extractor = self._extractors.get(ext)
        if not extractor:
            raise ValueError(f"Formato de arquivo não suportado: {ext}")
        return extractor

# Função auxiliar para manter compatibilidade simples na pipeline
def extract_file_data(file_input) -> pd.DataFrame:
    """Extrai dados de um caminho (str) ou de um objeto file-like (buffer)."""
    # Se for string, tratamos como caminho de arquivo
    if isinstance(file_input, str):
        if not os.path.exists(file_input):
            raise FileNotFoundError(f"Arquivo não encontrado: {file_input}")
        filename = file_input
    else:
        # Se for um objeto do Streamlit (UploadedFile), ele tem o atributo .name
        filename = getattr(file_input, "name", "file.csv")
    
    factory = ExtractorFactory()
    extractor = factory.get_extractor(filename)
    return extractor.extract(file_input)


# Mantemos de legado se ainda quiser puxar Dolar/Euro no futuro
def extract_api_data() -> dict:
    return {}
