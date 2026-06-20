"""Azure query helpers for mirror reports (issuer/year only — no SQL month filter)."""

from __future__ import annotations

from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from azure_reconciliation.azure_client import (
    _azure_settings,
    _pick_filter_column,
    list_table_columns,
)
from utils.logger import get_logger

logger = get_logger(__name__)

MONTH_SOURCE_PRIORITY: list[str] = [
    "GAA_Load_Date",
    "enrollment_last_update_date",
    "enrollee_last_update_date",
    "enrollment_create_date",
    "benefit_effective_date",
]

DIAGNOSTIC_DATE_COLUMNS: list[str] = [
    "GAA_Load_Date",
    "enrollment_create_date",
    "enrollment_last_update_date",
    "enrollee_last_update_date",
    "benefit_effective_date",
    "benefit_end_date",
]


def _table_ref(schema: str, table: str) -> str:
    return f"[{schema}].[{table}]"


def build_issuer_year_sql(
    columns: list[str],
    *,
    schema: str,
    table: str,
    issuer: str,
    year: str,
) -> tuple[str, dict[str, Any], str, str | None, str | None]:
    """Build SELECT for issuer + coverage_year only (no month filter)."""
    full_table = _table_ref(schema, table)
    issuer_col = _pick_filter_column(columns, ["hios_issuer_id", "issuer_id", "hios_id"])
    year_col = _pick_filter_column(columns, ["coverage_year", "plan_year", "benefit_year"])

    if not issuer_col:
        raise ValueError(f"Azure table {full_table} missing issuer column")

    clauses = [f"CAST([{issuer_col}] AS VARCHAR(20)) = :issuer"]
    params: dict[str, Any] = {"issuer": str(issuer)}

    if year_col:
        clauses.append(f"CAST([{year_col}] AS VARCHAR(4)) = :year")
        params["year"] = str(year)

    sql = f"SELECT * FROM {full_table} WHERE {' AND '.join(clauses)}"
    return sql, params, full_table, issuer_col, year_col


def fetch_enrollments_issuer_year(
    engine: Engine,
    *,
    issuer: str,
    year: str,
    schema: str | None = None,
    table: str | None = None,
    columns: list[str] | None = None,
) -> tuple[pd.DataFrame, str, dict[str, Any], str]:
    """Fetch rows for issuer + coverage_year only. Returns (df, sql, params, full_table)."""
    cfg = _azure_settings()
    schema = schema or cfg["schema"]
    table = table or cfg["table"]
    full_table = _table_ref(schema, table)

    cols = columns or list_table_columns(engine, schema, table)
    if not cols:
        logger.error("Azure table %s has no readable columns", full_table)
        return pd.DataFrame(), "", {}, full_table

    sql, params, _, _, _ = build_issuer_year_sql(
        cols, schema=schema, table=table, issuer=issuer, year=year
    )
    logger.info("Azure query SQL: %s", sql)
    logger.info("Azure query parameters: %s", params)

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)

    logger.info(
        "Azure raw row count returned: %d (issuer=%s year=%s)",
        len(df), issuer, year,
    )
    return df, sql, params, full_table


def resolve_month_source_column(
    columns: list[str],
    sample_df: pd.DataFrame | None = None,
) -> str | None:
    """Pick best month source: first priority column present with non-null data."""
    from azure_reconciliation.azure_mirror.columns import resolve_column

    for canonical in MONTH_SOURCE_PRIORITY:
        actual = resolve_column(columns, canonical)
        if not actual:
            continue
        if sample_df is not None and not sample_df.empty and actual in sample_df.columns:
            non_null = pd.to_datetime(sample_df[actual], errors="coerce").notna().sum()
            if non_null == 0:
                continue
        return canonical
    return None


def filter_by_month(
    df: pd.DataFrame,
    *,
    year: str,
    month: str,
    month_column_canonical: str,
) -> pd.DataFrame:
    """Filter issuer+year dataframe to rows matching calendar month on date column."""
    if df.empty:
        return df

    from azure_reconciliation.azure_mirror.columns import resolve_column

    actual = resolve_column(list(df.columns), month_column_canonical)
    if not actual:
        return pd.DataFrame()

    dates = pd.to_datetime(df[actual], errors="coerce")
    target_month = int(str(month).lstrip("0") or "0") or int(month)
    mask = dates.dt.month == target_month
    filtered = df.loc[mask].copy()
    if filtered.empty:
        logger.warning(
            "Month filter via %s returned 0 rows for %s/%s (base rows=%d)",
            month_column_canonical, year, month, len(df),
        )
    return filtered
