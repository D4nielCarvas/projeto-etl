import sys
import os
import io

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import pandas as pd
import pytest
from extract import (
    ExtractorFactory,
    CSVExtractor,
    ExcelExtractor,
    XMLExtractor,
    extract_file_data,
)

class TestExtractorFactory:
    def test_get_extractor_csv(self):
        factory = ExtractorFactory()
        extractor = factory.get_extractor("arquivo.csv")
        assert isinstance(extractor, CSVExtractor)

    def test_get_extractor_xlsx(self):
        factory = ExtractorFactory()
        extractor = factory.get_extractor("dados.xlsx")
        assert isinstance(extractor, ExcelExtractor)

    def test_get_extractor_xml(self):
        factory = ExtractorFactory()
        extractor = factory.get_extractor("export.xml")
        assert isinstance(extractor, XMLExtractor)

    def test_get_extractor_unsupported(self):
        factory = ExtractorFactory()
        with pytest.raises(ValueError, match="Formato de arquivo não suportado"):
            factory.get_extractor("imagem.png")

def test_extract_file_data_file_not_found():
    with pytest.raises(FileNotFoundError):
        extract_file_data("nao_existe.csv")

def test_extract_file_data_mock_csv(mocker):
    # Mock do pd.read_csv para simular a extração
    mock_df = pd.DataFrame({"col1": [1, 2]})
    mocker.patch("pandas.read_csv", return_value=mock_df)
    
    # Precisamos criar um arquivo fake no disco ou usar mock para exist
    mocker.patch("os.path.exists", return_value=True)
    
    df = extract_file_data("fake.csv")
    assert df.equals(mock_df)

def test_extract_file_data_streamlit_upload_mock(mocker):
    # Simula um UploadedFile do Streamlit
    class MockUploadedFile:
        def __init__(self, name):
            self.name = name

    mock_df = pd.DataFrame({"col1": [1, 2]})
    mocker.patch("pandas.read_excel", return_value=mock_df)
    
    fake_upload = MockUploadedFile("planilha.xlsx")
    df = extract_file_data(fake_upload)
    assert df.equals(mock_df)
