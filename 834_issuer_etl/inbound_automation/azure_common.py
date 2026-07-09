"""Shared Azure connection helpers for inbound automation (DDL + load)."""

from __future__ import annotations

import os
import sys

from sqlalchemy.engine import Engine

from azure_reconciliation.azure_client import get_engine, test_azure_connection_query
from config.config import reload_env, settings

TRUTHY = frozenset({"true", "1", "yes", "y", "t"})

INBOUND_TABLES = (
    "inbound_automation",
    "inbound_automation_run_log",
    "inbound_automation_file_log",
)


def env_enabled() -> bool:
    raw = os.getenv("INBOUND_AUTOMATION_ENABLED", "").strip().strip('"').strip("'")
    return bool(raw) and raw.lower() in TRUTHY


def require_env_gate(action: str) -> None:
    settings.refresh_from_env()
    reload_env()
    if not env_enabled():
        print(
            f"INBOUND_AUTOMATION_ENABLED is missing or not 'true'. "
            f"Failing safely — no Azure {action}.",
            file=sys.stderr,
        )
        raise SystemExit(1)


def presence_flags() -> dict[str, bool]:
    settings.refresh_from_env()
    reload_env()
    return {
        "SERVER present": bool(os.getenv("SERVER", "").strip()),
        "DATABASE present": bool(os.getenv("DATABASE", "").strip()),
        "USERNAME present": bool(os.getenv("USERNAME", "").strip()),
        "DRIVER present": bool(os.getenv("DRIVER", "").strip()),
        "AZURE_SQL_SCHEMA present": bool(os.getenv("AZURE_SQL_SCHEMA", "").strip()),
    }


def connect_automation_engine() -> Engine:
    """Connect using the existing project Azure config pattern."""
    flags = presence_flags()
    print("Azure config presence (no secrets):")
    for key, present in flags.items():
        print(f"  {key}: {present}")

    if not (flags["SERVER present"] and flags["DATABASE present"] and flags["USERNAME present"]):
        print(
            "Azure config missing: SERVER, DATABASE, and USERNAME are required.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    try:
        engine = get_engine()
        test_azure_connection_query(engine)
        print("Azure connection successful")
        return engine
    except Exception as exc:
        print(f"Azure connection failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
