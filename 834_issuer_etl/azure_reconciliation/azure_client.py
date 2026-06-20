"""
Read-only Azure SQL access via SQLAlchemy.

Never INSERT/UPDATE/DELETE/ALTER. SELECT only.

Connection modes:
  - PASSWORD set  → UID + PWD (SQL auth)
  - PASSWORD empty/missing → passwordless (Trusted_Connection / AAD integrated / MSI)
"""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

from config.config import ENV_FILE
from dotenv import load_dotenv
from utils.logger import get_logger

logger = get_logger(__name__)

load_dotenv(ENV_FILE, override=True)

DEFAULT_ENROLLMENTS_TABLE = "Enrollments_PY2026"
DEFAULT_SCHEMA = "dbo"
DEFAULT_DRIVER = "ODBC Driver 17 for SQL Server"

# Placeholder values treated as "no password"
_PASSWORD_PLACEHOLDERS = frozenset({
    "",
    "none",
    "null",
    "nil",
    "n/a",
    "na",
    "-",
    "undefined",
    "your-password",
    "your_password",
    "dummy",
    "dummy_password",
    "changeme",
    "password",
    "empty",
})

# Cached working ODBC string after successful verify (passwordless may try several modes)
_cached_odbc_connect: str | None = None
_cached_auth_mode: str | None = None

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


def _reload_env() -> None:
    load_dotenv(ENV_FILE, override=True)


def _env(key: str, legacy: str = "") -> str:
    _reload_env()
    val = os.getenv(key) or os.getenv(legacy) or ""
    return str(val).strip().strip('"').strip("'")


def _is_real_password(value: str) -> bool:
    """True only when PASSWORD is a non-empty, non-placeholder value."""
    cleaned = str(value or "").strip().strip('"').strip("'")
    return cleaned.lower() not in _PASSWORD_PLACEHOLDERS


def _azure_settings() -> dict[str, str]:
    raw_password = _env("AZURE_SQL_PASSWORD", "PASSWORD")
    return {
        "server": _env("AZURE_SQL_SERVER", "SERVER"),
        "database": _env("AZURE_SQL_DATABASE", "DATABASE"),
        "username": _env("AZURE_SQL_USERNAME", "USERNAME"),
        "password": raw_password if _is_real_password(raw_password) else "",
        "driver": _env("AZURE_SQL_DRIVER", "DRIVER") or DEFAULT_DRIVER,
        "table": os.getenv("AZURE_ENROLLMENTS_TABLE", DEFAULT_ENROLLMENTS_TABLE),
        "schema": os.getenv("AZURE_SQL_SCHEMA", DEFAULT_SCHEMA),
    }


def get_connection_meta() -> dict[str, Any]:
    """Return connection mode diagnostics (no secrets)."""
    cfg = _azure_settings()
    password_present = bool(cfg["password"])
    return {
        "password_mode": "password" if password_present else "passwordless",
        "auth_mode": _cached_auth_mode,
        "driver": cfg["driver"],
        "server": cfg["server"],
        "database": cfg["database"],
        "username_present": bool(cfg["username"]),
        "password_present": password_present,
    }


def log_connection_failure(meta: dict[str, Any], exc: Exception | str | None = None) -> None:
    """Log exact connection mode used when Azure connection fails."""
    msg = (
        "Azure connection failed — password_mode=%s auth_mode=%s driver=%s server=%s "
        "database=%s username_present=%s password_present=%s"
    )
    args = (
        meta.get("password_mode"),
        meta.get("auth_mode"),
        meta.get("driver"),
        meta.get("server"),
        meta.get("database"),
        meta.get("username_present"),
        meta.get("password_present"),
    )
    if exc is not None:
        logger.warning(msg + " error=%s", *args, exc)
    else:
        logger.warning(msg, *args)


def azure_configured() -> bool:
    """True when SERVER and DATABASE are set. PASSWORD is never required."""
    cfg = _azure_settings()
    return bool(cfg["server"] and cfg["database"])


def _encrypt_suffix() -> str:
    return "Encrypt=yes;TrustServerCertificate=yes;"


def _odbc_base(cfg: dict[str, str]) -> str:
    return (
        f"DRIVER={{{cfg['driver']}}};"
        f"SERVER={cfg['server']};"
        f"DATABASE={cfg['database']};"
    )


def build_password_connect_string(cfg: dict[str, str]) -> str:
    """SQL authentication with UID + PWD."""
    return (
        _odbc_base(cfg)
        + f"UID={cfg['username']};"
        + f"PWD={cfg['password']};"
        + _encrypt_suffix()
    )


def passwordless_connect_candidates(cfg: dict[str, str]) -> list[tuple[str, str]]:
    """
    Passwordless ODBC strings to try in order.

    Uses USERNAME/DRIVER when present; never includes PWD.
    """
    base = _odbc_base(cfg)
    enc = _encrypt_suffix()
    username = cfg.get("username") or ""
    candidates: list[tuple[str, str]] = []

    # 1) User-specified trusted connection (Windows integrated auth)
    candidates.append(("trusted_connection", base + "Trusted_Connection=yes;" + enc))

    # 2) Integrated Security SSPI (alias used by some drivers)
    candidates.append(("integrated_security_sspi", base + "Integrated Security=SSPI;" + enc))

    # 3) Azure AD integrated — with UID when USERNAME is set
    if username:
        candidates.append((
            "active_directory_integrated_uid",
            base + f"UID={username};Authentication=ActiveDirectoryIntegrated;" + enc,
        ))

    # 4) Azure AD integrated — current identity
    candidates.append((
        "active_directory_integrated",
        base + "Authentication=ActiveDirectoryIntegrated;" + enc,
    ))

    # 5) Azure managed identity (no username/password)
    candidates.append(("active_directory_msi", base + "Authentication=ActiveDirectoryMsi;" + enc))

    # 6) Username only, trusted connection (some on-prem SQL setups)
    if username:
        candidates.append((
            "trusted_connection_uid",
            base + f"UID={username};Trusted_Connection=yes;" + enc,
        ))

    return candidates


def build_odbc_connect_string() -> str:
    """
    Build pyodbc connection string.

    Returns cached string from a successful verify when available.
    """
    global _cached_odbc_connect
    if _cached_odbc_connect:
        return _cached_odbc_connect

    cfg = _azure_settings()
    if not azure_configured():
        raise RuntimeError(
            "Azure SQL connection settings missing. Set SERVER and DATABASE in .env"
        )

    if cfg["password"]:
        return build_password_connect_string(cfg)

    # Default passwordless: first candidate (trusted_connection)
    return passwordless_connect_candidates(cfg)[0][1]


def build_connection_url(conn_str: str | None = None) -> str:
    conn_str = conn_str or build_odbc_connect_string()
    return "mssql+pyodbc:///?odbc_connect=" + quote_plus(conn_str)


def _engine_from_conn_str(conn_str: str) -> Engine:
    return create_engine(build_connection_url(conn_str), future=True)


def get_engine() -> Engine:
    return _engine_from_conn_str(build_odbc_connect_string())


def _try_connect(conn_str: str) -> Engine:
    engine = _engine_from_conn_str(conn_str)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return engine


def verify_azure_connection() -> tuple[Engine | None, dict[str, Any]]:
    """
    Test Azure connectivity. Returns (engine, meta).

    Passwordless mode tries multiple auth strategies until one succeeds.
    Logs ``Azure connection successful`` on success.
    Never raises.
    """
    global _cached_odbc_connect, _cached_auth_mode

    _cached_odbc_connect = None
    _cached_auth_mode = None

    meta = get_connection_meta()
    if not azure_configured():
        meta["connected"] = False
        meta["error"] = "SERVER and DATABASE required"
        log_connection_failure(meta, meta["error"])
        return None, meta

    cfg = _azure_settings()
    logger.info(
        "Azure connecting — password_mode=%s driver=%s server=%s database=%s "
        "username_present=%s password_present=%s",
        meta["password_mode"],
        meta["driver"],
        meta["server"],
        meta["database"],
        meta["username_present"],
        meta["password_present"],
    )

    try:
        if cfg["password"]:
            conn_str = build_password_connect_string(cfg)
            engine = _try_connect(conn_str)
            _cached_odbc_connect = conn_str
            _cached_auth_mode = "sql_auth"
        else:
            last_exc: Exception | None = None
            engine = None
            for auth_mode, conn_str in passwordless_connect_candidates(cfg):
                try:
                    engine = _try_connect(conn_str)
                    _cached_odbc_connect = conn_str
                    _cached_auth_mode = auth_mode
                    logger.info("Azure passwordless auth succeeded: auth_mode=%s", auth_mode)
                    break
                except Exception as exc:
                    last_exc = exc
                    logger.debug("Azure auth_mode=%s failed: %s", auth_mode, exc)
            if engine is None:
                raise last_exc or RuntimeError("All passwordless auth modes failed")

        meta = get_connection_meta()
        meta["connected"] = True
        meta["auth_mode"] = _cached_auth_mode
        logger.info("Azure connection successful")
        return engine, meta

    except Exception as exc:
        meta = get_connection_meta()
        meta["connected"] = False
        meta["error"] = str(exc)
        log_connection_failure(meta, exc)
        return None, meta


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
