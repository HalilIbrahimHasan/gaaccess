"""Shared DataFrame column helpers — avoid str/Series confusion."""

from __future__ import annotations

import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)


def zmonth(m: str) -> str:
    return str(m).strip().zfill(2)


def col_series(df: pd.DataFrame, *names: str, default: str = "") -> pd.Series:
    """
    Return the first existing column as a Series.
    Never return a bare string (fixes 'str' object has no attribute apply/astype).
    """
    if df is None or df.empty:
        return pd.Series(dtype=object)
    for name in names:
        if name and name in df.columns:
            return df[name]
    logger.debug(
        "Column(s) %s not in dataframe (cols=%s) — using default",
        names, list(df.columns)[:12],
    )
    return pd.Series([default] * len(df), index=df.index)
