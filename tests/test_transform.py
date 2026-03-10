"""
Testes unitários para o módulo transform.py.

Complexidade: O(n) para a maioria das operações de transformação.
Execute com: pytest tests/ -v
"""
import sys
import os

# Garante que o diretório scripts/ está no path para imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import pandas as pd
import pytest

from transform import (
    _normalize_column_name,
    _try_parse_br_numeric,
    _detect_and_parse_dates,
    _detect_and_parse_numerics,
    clean_generic_dataframe,
    profile_dataframe,
    transform_data,
)


# ── _normalize_column_name ────────────────────────────────────────────────────

class TestNormalizeColumnName:
    def test_remove_accent_and_lowercase(self):
        assert _normalize_column_name("Número de Mês") == "numero_de_mes"

    def test_spaces_become_underscores(self):
        assert _normalize_column_name("  Total Vendas  ") == "total_vendas"

    def test_special_chars_removed(self):
        assert _normalize_column_name("Valor (R$)") == "valor_r"

    def test_empty_string_returns_fallback(self):
        assert _normalize_column_name("") == "col_sem_nome"

    def test_already_normalized(self):
        assert _normalize_column_name("coluna_normal") == "coluna_normal"

    def test_numeric_name_preserved(self):
        assert _normalize_column_name("col2024") == "col2024"


# ── _try_parse_br_numeric ─────────────────────────────────────────────────────

class TestTryParseBrNumeric:
    def test_converts_br_format(self):
        s = pd.Series(["1.234,56", "2.000,00", "300,00"])
        result = _try_parse_br_numeric(s)
        assert result.tolist() == pytest.approx([1234.56, 2000.00, 300.00])

    def test_handles_negative_br_format(self):
        s = pd.Series(["-1.500,00", "2.000,00"])
        result = _try_parse_br_numeric(s)
        assert result.iloc[0] == pytest.approx(-1500.00)

    def test_falls_back_to_international_format(self):
        s = pd.Series(["1234.56", "200.00"])
        result = _try_parse_br_numeric(s)
        assert result.tolist() == pytest.approx([1234.56, 200.00])

    def test_nan_on_unparseable(self):
        s = pd.Series(["abc", "xyz"])
        result = _try_parse_br_numeric(s)
        assert result.isna().all()

    def test_empty_series(self):
        result = _try_parse_br_numeric(pd.Series([], dtype=str))
        assert len(result) == 0


# ── _detect_and_parse_dates ───────────────────────────────────────────────────

class TestDetectAndParseDates:
    def test_detects_date_column_by_name_and_value(self):
        df = pd.DataFrame({"data_venda": ["01/01/2024", "15/06/2024", "20/12/2024"]})
        result = _detect_and_parse_dates(df)
        assert pd.api.types.is_datetime64_any_dtype(result["data_venda"])

    def test_skips_non_date_column(self):
        df = pd.DataFrame({"produto": ["Notebook", "Mouse", "Teclado"]})
        result = _detect_and_parse_dates(df)
        assert result["produto"].dtype == object

    def test_accepts_iso_format(self):
        df = pd.DataFrame({"data_registro": ["2024-01-01", "2024-06-15"]})
        result = _detect_and_parse_dates(df)
        assert pd.api.types.is_datetime64_any_dtype(result["data_registro"])

    def test_skips_column_already_datetime(self):
        df = pd.DataFrame({"dt": pd.to_datetime(["2024-01-01"])})
        result = _detect_and_parse_dates(df)
        assert pd.api.types.is_datetime64_any_dtype(result["dt"])


# ── clean_generic_dataframe ───────────────────────────────────────────────────

class TestCleanGenericDataframe:
    def test_removes_exact_duplicates(self):
        df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
        result = clean_generic_dataframe(df)
        assert len(result) == 2

    def test_normalizes_column_names(self):
        df = pd.DataFrame({"Coluna Inválida!": [1, 2]})
        result = clean_generic_dataframe(df)
        assert "coluna_invalida" in result.columns

    def test_drops_fully_null_columns(self):
        df = pd.DataFrame({"a": [1, 2], "b": [None, None]})
        result = clean_generic_dataframe(df)
        assert "b" not in result.columns

    def test_strips_string_whitespace(self):
        df = pd.DataFrame({"nome": ["  Alice  ", " Bob "]})
        result = clean_generic_dataframe(df)
        assert result["nome"].tolist() == ["Alice", "Bob"]

    def test_reset_index_after_cleaning(self):
        df = pd.DataFrame({"a": [1, 1, 2]})
        result = clean_generic_dataframe(df)
        assert result.index.tolist() == list(range(len(result)))


# ── profile_dataframe ─────────────────────────────────────────────────────────

class TestProfileDataframe:
    def test_returns_required_keys(self):
        df = pd.DataFrame({"a": [1.0, 2.0], "b": ["x", "y"]})
        profile = profile_dataframe(df)
        required = {
            "total_rows", "total_cols", "numeric_cols", "categorical_cols",
            "date_cols", "null_counts", "null_pct", "unique_counts",
            "completeness_pct", "numeric_stats",
        }
        assert required.issubset(profile.keys())

    def test_completeness_100_when_no_nulls(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        profile = profile_dataframe(df)
        assert profile["completeness_pct"] == 100.0

    def test_completeness_partial_with_nulls(self):
        df = pd.DataFrame({"a": [1, None], "b": ["x", "y"]})
        profile = profile_dataframe(df)
        assert profile["completeness_pct"] < 100.0

    def test_numeric_cols_identified(self):
        df = pd.DataFrame({"valor": [1.0, 2.0], "nome": ["a", "b"]})
        profile = profile_dataframe(df)
        assert "valor" in profile["numeric_cols"]
        assert "nome" not in profile["numeric_cols"]


# ── transform_data (integração) ───────────────────────────────────────────────

class TestTransformDataIntegration:
    def test_returns_tuple_df_and_profile(self):
        df = pd.DataFrame({
            "Data Venda": ["01/01/2024", "02/01/2024"],
            "Valor (R$)": ["1.000,00", "2.500,50"],
            "Produto": ["Notebook", "Mouse"],
        })
        result_df, profile = transform_data(df)
        assert isinstance(result_df, pd.DataFrame)
        assert isinstance(profile, dict)

    def test_br_value_column_becomes_float(self):
        df = pd.DataFrame({
            "valor": ["1.000,00", "2.500,50"],
        })
        result_df, _ = transform_data(df)
        assert pd.api.types.is_float_dtype(result_df["valor"])

    def test_profile_rows_match_cleaned_df(self):
        df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
        result_df, profile = transform_data(df)
        assert profile["total_rows"] == len(result_df)
