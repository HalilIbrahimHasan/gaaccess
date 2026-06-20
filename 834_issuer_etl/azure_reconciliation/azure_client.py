"""
Read-only Azure SQL access via SQLAlchemy.

Never INSERT/UPDATE/DELETE/ALTER. SELECT only.
"""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

from config.config import PROJECT_ROOT, ENV_FILE
from dotenv import load_dotenv
from utils.logger import get_logger

logger = get_logger(__name__)

load_dotenv(ENV_FILE, override=True)

DEFAULT_ENROLLMENTS_TABLE = "Enrollments_PY2026"
DEFAULT_SCHEMA = "dbo"

AZURE_TABLE_CANDIDATES = [
    "Enrollments_PY2026",
    "Enrollments_PY2526_01312026",
    "Enrollments_PY2526_02272026",
    "Enrollments_PY2526_03312026",
    "Enrollments_PY2526_04302026",
    "Enrollments_PY2526_FEB10",
    "Enrollments_PY2526_DEC31",
    "DuplicateEnrollment_Overlap",
    "834_Inbound_test",
    "834_Inbound_header_test",
    "monthly_discrepancy",
    "monthly_discrepancy_PY2026",
    "CarrierInvoice",
    "CarrierInvoice_test",
]


def _azure_settings() -> dict[str, str]:
    return {
        "server": os.getenv("AZURE_SQL_SERVER") or os.getenv("SERVER", ""),
        "database": os.getenv("AZURE_SQL_DATABASE") or os.getenv("DATABASE", ""),
        "username": os.getenv("AZURE_SQL_USERNAME") or os.getenv("USERNAME", ""),
        "password": os.getenv("AZURE_SQL_PASSWORD") or os.getenv("PASSWORD", ""),
        "driver": os.getenv("AZURE_SQL_DRIVER") or os.getenv("DRIVER", "ODBC Driver 18 for SQL Server"),
        "table": os.getenv("AZURE_ENROLLMENTS_TABLE", DEFAULT_ENROLLMENTS_TABLE),
        "schema": os.getenv("AZURE_SQL_SCHEMA", DEFAULT_SCHEMA),
    }


def azure_configured() -> bool:
    cfg = _azure_settings()
    return bool(cfg["server"] and cfg["database"] and cfg["username"] and cfg["password"])


def build_connection_url() -> str:
    cfg = _azure_settings()
    if not azure_configured():
        raise RuntimeError(
            "Azure SQL credentials missing. Set SERVER, DATABASE, USERNAME, PASSWORD in .env"
        )
    driver = quote_plus(cfg["driver"])
    conn = (
        f"mssql+pyodbc://{quote_plus(cfg['username'])}:{quote_plus(cfg['password'])}"
        f"@{cfg['server']}/{cfg['database']}?driver={driver}"
        "&TrustServerCertificate=yes"
    )
    return conn


def get_engine() -> Engine:
    return create_engine(build_connection_url(), future=True)


def list_table_columns(engine: Engine, schema: str, table: str) -> list[str]:
    try:
        insp = inspect(engine)
        cols = insp.get_columns(table, schema=schema)
        return [c["name"] for c in cols]
    except Exception as exc:
        logger.warning("Could not inspect %s.%s: %s", schema, table, exc)
        return []


def list_available_tables(engine: Engine, schema: str = DEFAULT_SCHEMA) -> list[str]:
    try:
        insp = inspect(engine)
        return sorted(insp.get_table_names(schema=schema))
    except Exception as exc:
        logger.warning("Could not list Azure tables: %s", exc)
        return []


def _pick_filter_column(columns: list[str], candidates: list[str]) -> str | None:
    norm = {_c.lower(): _c for _c in columns}
    for cand in candidates:
        if cand.lower() in norm:
            return norm[cand.lower()]
    return None


def fetch_enrollments_partition(
    engine: Engine,
    *,
    issuer: str,
    year: str,
    month: str,
    schema: str | None = None,
    table: str | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    """
    SELECT-only query for one issuer/year/month partition.

    Dynamically picks filter columns present in the table.
    """
    cfg = _azure_settings()
    schema = schema or cfg["schema"]
    table = table or cfg["table"]
    full_table = f"[{schema}].[{table}]"

    columns = list_table_columns(engine, schema, table)
    if not columns:
        logger.error("Azure table %s has no readable columns", full_table)
        return pd.DataFrame()

    issuer_col = _pick_filter_column(columns, ["hios_issuer_id", "issuer_id", "hios_id"])
    year_col = _pick_filter_column(columns, ["coverage_year", "plan_year", "benefit_year"])
    month_cols = [
        _pick_filter_column(columns, c)
        for c in [
            ["enrollment_last_update_date"],
            ["enrollee_last_update_date"],
            ["benefit_effective_date"],
            ["enrollment_create_date"],
        ]
    ]
    month_col = next((c for c in month_cols if c), None)

    if not issuer_col:
        logger.warning("Azure table missing issuer column — returning empty frame")
        return pd.DataFrame()

    clauses = [f"[{issuer_col}] = :issuer"]
    params: dict[str, Any] = {"issuer": issuer}

    if year_col:
        clauses.append(f"CAST([{year_col}] AS VARCHAR(4)) = :year")
        params["year"] = str(year)

    # Month filter: use date column month extraction when available
    if month_col:
        clauses.append(
            f"(MONTH([{month_col}]) = :month_int OR FORMAT([{month_col}], 'MM') = :month_str)"
        )
        params["month_int"] = int(month)
        params["month_str"] = str(month).zfill(2)

    where = " AND ".join(clauses)
    sql = f"SELECT * FROM {full_table} WHERE {where}"
    if limit:
        sql += f" ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH NEXT {int(limit)} ROWS ONLY"

    logger.info("Azure query: %s params=%s", sql[:200], params)
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)
    logger.info("Azure rows fetched: %d for %s/%s/%s", len(df), issuer, year, month)
    return df


def fetch_enrollments_issuer(
    engine: Engine,
    *,
    issuer: str,
    years: list[str] | None = None,
    schema: str | None = None,
    table: str | None = None,
) -> pd.DataFrame:
    """Fetch all rows for an issuer (optional year filter) for lifecycle comparison."""
    cfg = _azure_settings()
    schema = schema or cfg["schema"]
    table = table or cfg["table"]
    full_table = f"[{schema}].[{table}]"
    columns = list_table_columns(engine, schema, table)
    issuer_col = _pick_filter_column(columns, ["hios_issuer_id", "issuer_id", "hios_id"])
    year_col = _pick_filter_column(columns, ["coverage_year", "plan_year"])

    if not issuer_col:
        return pd.DataFrame()

    clauses = [f"[{issuer_col}] = :issuer"]
    params: dict[str, Any] = {"issuer": issuer}
    if years and year_col:
        placeholders = ", ".join(f":y{i}" for i in range(len(years)))
        clauses.append(f"CAST([{year_col}] AS VARCHAR(4)) IN ({placeholders})")
        for i, y in enumerate(years):
            params[f"y{i}"] = str(y)

    sql = f"SELECT * FROM {full_table} WHERE {' AND '.join(clauses)}"
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)
