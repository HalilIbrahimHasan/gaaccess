"""Azure DDL executor for inbound automation Phase 2A (create tables only).

Safety:
- Executes only the approved DDL file `sql/inbound_automation_ddl.sql`.
- No INSERT/UPDATE/DELETE.
- No DROP/TRUNCATE/ALTER existing production tables (DDL uses IF NOT EXISTS).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from sqlalchemy.engine import Engine

from azure_reconciliation.azure_client import (
    azure_configured,
    get_engine,
    test_azure_connection_query,
 )
from config.config import reload_env, settings


TRUTHY = frozenset({"true", "1", "yes", "y", "t"})


def _env_enabled() -> bool:
    raw = os.getenv("INBOUND_AUTOMATION_ENABLED", "").strip().strip('"').strip("'")
    if not raw:
        return False
    return raw.lower() in TRUTHY


def _presence_flags() -> dict[str, bool]:
    """
    Presence flags for the *existing* Azure configuration variables.

    Do not print values (avoid secrets). Only presence.
    """
    # Ensure .env is loaded/reloaded.
    settings.refresh_from_env()
    reload_env()
    return {
        "SERVER present": bool(os.getenv("SERVER", "").strip()),
        "DATABASE present": bool(os.getenv("DATABASE", "").strip()),
        "USERNAME present": bool(os.getenv("USERNAME", "").strip()),
        "DRIVER present": bool(os.getenv("DRIVER", "").strip()),
        "AZURE_SQL_SCHEMA present": bool(os.getenv("AZURE_SQL_SCHEMA", "").strip()),
    }


def _split_sql_batches(sql_text: str) -> list[str]:
    """
    Split SQL script into batches separated by a standalone `GO` line.

    `GO` is not understood by SQL Server via raw ODBC, so we split batches.
    """
    batches: list[str] = []
    buf: list[str] = []

    for line in sql_text.splitlines():
        if line.strip().upper() == "GO":
            batch = "\n".join(buf).strip()
            if batch:
                batches.append(batch)
            buf = []
        else:
            buf.append(line)

    tail = "\n".join(buf).strip()
    if tail:
        batches.append(tail)
    return batches


def _execute_ddl_file(*, engine: Engine, ddl_path: Path) -> None:
    ddl_text = ddl_path.read_text(encoding="utf-8")
    batches = _split_sql_batches(ddl_text)
    if not batches:
        raise RuntimeError(f"No SQL batches found in DDL file: {ddl_path}")

    with engine.connect() as conn:
        for i, batch in enumerate(batches, start=1):
            # Use execute directly; SQL is pre-approved and parameterless.
            conn.exec_driver_sql(batch)
            print(f"Executed DDL batch {i}/{len(batches)}")


def _table_exists_sql(schema: str, table: str) -> str:
    # Use OBJECT_ID with schema-qualified name.
    return f"SELECT CASE WHEN OBJECT_ID(N'{schema}.{table}', N'U') IS NULL THEN 0 ELSE 1 END AS exists_flag"


def _row_count_sql(schema: str, table: str) -> str:
    return f"SELECT COUNT(1) AS row_count FROM [{schema}].[{table}]"


def _run_scalar(engine: Engine, sql: str) -> int:
    with engine.connect() as conn:
        row = conn.exec_driver_sql(sql).fetchone()
        if row is None:
            return 0
        return int(row[0])


def create_phase2a_tables(*, ddl_path: Path, runner_name: str = "inbound_automation_phase2a") -> int:
    # Always reload env/flags at runner start.
    settings.refresh_from_env()

    if not _env_enabled():
        print(
            "INBOUND_AUTOMATION_ENABLED is missing or not 'true'. "
            "Failing safely and not executing any Azure DDL.",
            file=sys.stderr,
        )
        return 1

    flags = _presence_flags()
    print("Azure config presence (no secrets):")
    for k, v in flags.items():
        print(f"  {k}: {v}")

    # Require the legacy/working Azure settings to be present before attempting login.
    if not (flags["SERVER present"] and flags["DATABASE present"] and flags["USERNAME present"]):
        print(
            "Azure config missing: SERVER, DATABASE, and USERNAME are required for --create-table.",
            file=sys.stderr,
        )
        return 1

    # Use the existing Azure connection pattern from azure_client.py.
    try:
        engine = get_engine()
        test_azure_connection_query(engine)  # forces interactive login
    except Exception as exc:
        print(f"Azure connection failed: {exc}", file=sys.stderr)
        return 1

    schema = "dbo"
    tables = [
        "inbound_automation",
        "inbound_automation_run_log",
        "inbound_automation_file_log",
    ]

    print("Pre-check table existence:")
    pre_exists: dict[str, int] = {}
    for t in tables:
        sql = _table_exists_sql(schema, t)
        flag = _run_scalar(engine, sql)
        pre_exists[t] = flag
        state = "EXISTS" if flag else "MISSING"
        print(f"  {schema}.{t}: {state}")

    print(f"Executing approved DDL from: {ddl_path}")
    _execute_ddl_file(engine=engine, ddl_path=ddl_path)

    print("Post-check table existence + row counts:")
    with engine.connect() as conn:
        # Build a single query per table for clarity.
        for t in tables:
            exists_flag = _run_scalar(engine, _table_exists_sql(schema, t))
            row_count = _run_scalar(engine, _row_count_sql(schema, t))
            created_or_existing = (
                "created (was missing)" if pre_exists.get(t, 0) == 0 and exists_flag == 1 else "already existed"
            )
            print(f"  {schema}.{t}: exists={exists_flag} row_count={row_count} ({created_or_existing})")

    print("\nVerification SQL (run manually in Azure if desired):")
    for t in tables:
        print(f"- Table exists: {schema}.{t}")
        print(f"  {_table_exists_sql(schema, t)}")
        print(f"- Row count (should be 0 for fresh tables): {schema}.{t}")
        print(f"  {_row_count_sql(schema, t)}")

    return 0

