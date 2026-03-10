import logging
import re
import unicodedata
from typing import Tuple

import pandas as pd

logger = logging.getLogger(__name__)

_DATE_KEYWORDS = {
    "data", "date", "dt", "mes", "ano", "periodo",
    "competencia", "vencimento", "criacao", "atualizacao",
    "emissao", "entrega", "cadastro", "registro",
}

_DATE_FORMATS = [
    "%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d",
    "%d-%m-%Y", "%Y/%m/%d", "%m/%Y", "%Y-%m",
]


def _normalize_column_name(name: str) -> str:
    """Normaliza nomes: lowercase, sem acentos, underscores."""
    name = str(name).strip()
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", "_", name).lower()
    return name or "col_sem_nome"


def _try_parse_br_numeric(series: pd.Series) -> pd.Series:
    """Detecta e converte formato brasileiro (1.234,56) para float."""
    sample = series.dropna().astype(str).head(100)
    br_matches = sample.str.match(r"^-?[\d\.]+,\d+$").sum()
    if br_matches / max(len(sample), 1) > 0.5:
        cleaned = (
            series.astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        return pd.to_numeric(cleaned, errors="coerce")
    return pd.to_numeric(series, errors="coerce")


def _detect_and_parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Detecta colunas de data por nome heuristico e amostragem de conteudo."""
    for col in df.columns:
        if df[col].dtype != "object":
            continue

        col_lower = col.lower()
        is_date_named = any(kw in col_lower for kw in _DATE_KEYWORDS)
        sample = df[col].dropna().head(50)
        if len(sample) == 0:
            continue

        parsed = None
        for fmt in _DATE_FORMATS:
            try:
                candidate = pd.to_datetime(sample, format=fmt, errors="coerce")
                if candidate.notna().sum() / len(sample) > 0.7:
                    parsed = pd.to_datetime(df[col], format=fmt, errors="coerce")
                    break
            except Exception:
                continue

        if parsed is None and is_date_named:
            try:
                # infer_datetime_format foi removido no Pandas 2.0 — inferência é padrão
                candidate = pd.to_datetime(sample, errors="coerce")
                if candidate.notna().sum() / len(sample) > 0.6:
                    parsed = pd.to_datetime(df[col], errors="coerce")
            except Exception:
                pass

        if parsed is not None:
            df[col] = parsed

    return df


def _detect_and_parse_numerics(df: pd.DataFrame) -> pd.DataFrame:
    """Converte colunas string com conteudo numerico (padrao BR ou internacional)."""
    for col in df.columns:
        if df[col].dtype != "object":
            continue
        sample = df[col].dropna().head(100)
        if len(sample) == 0:
            continue

        converted = _try_parse_br_numeric(sample)
        if converted.notna().sum() / len(sample) > 0.7:
            df[col] = _try_parse_br_numeric(df[col])

    return df


def profile_dataframe(df: pd.DataFrame) -> dict:
    """Gera relatorio de perfil do DataFrame para uso no dashboard."""
    total_cells = len(df) * len(df.columns)
    null_sum    = int(df.isnull().sum().sum())
    completeness = round((1 - null_sum / max(total_cells, 1)) * 100, 1)
    numeric_cols = df.select_dtypes(include="number").columns.tolist()

    return {
        "total_rows":      len(df),
        "total_cols":      len(df.columns),
        "numeric_cols":    numeric_cols,
        "categorical_cols": df.select_dtypes(include=["object", "category"]).columns.tolist(),
        "date_cols":       df.select_dtypes(include="datetime").columns.tolist(),
        "null_counts":     df.isnull().sum().to_dict(),
        "null_pct":        (df.isnull().sum() / max(len(df), 1) * 100).round(1).to_dict(),
        "unique_counts":   df.nunique().to_dict(),
        "completeness_pct": completeness,
        "numeric_stats":   df[numeric_cols].describe().to_dict() if numeric_cols else {},
    }


def clean_generic_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Limpeza aprimorada: normaliza nomes, datas, numericos e strings."""
    df.columns = [_normalize_column_name(c) for c in df.columns]

    before = len(df)
    df = df.drop_duplicates().copy()
    logger.info("Duplicatas removidas: %d", before - len(df))

    df = df.dropna(axis=1, how="all")
    df = _detect_and_parse_dates(df)
    df = _detect_and_parse_numerics(df)

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"nan": None, "None": None, "": None})

    df.reset_index(drop=True, inplace=True)
    return df


def transform_data(df: pd.DataFrame, cotacoes: dict = None) -> Tuple[pd.DataFrame, dict]:
    """Pipeline de transformacao generica. Retorna (df_limpo, perfil_do_dataset)."""
    df_transformed = clean_generic_dataframe(df)
    profile        = profile_dataframe(df_transformed)
    logger.info(
        "Transformacao finalizada: %d registros, %d colunas. Completude: %.1f%%",
        profile["total_rows"],
        profile["total_cols"],
        profile["completeness_pct"],
    )
    return df_transformed, profile
