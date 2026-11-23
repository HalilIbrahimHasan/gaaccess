"""Azure connection helpers for outbound automation (own env gate / batch settings)."""

from __future__ import annotations

import os
import sys

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from azure_reconciliation.azure_client import build_connection_url, test_azure_connection_query
from config.config import reload_env, settings
from outbound_automation.constants import ENV_BATCH_SIZE, ENV_ENABLED, ENV_FAST_EXECUTEMANY

TRUTHY = frozenset({"true", "1", "yes", "y", "t"})

DEFAULT_BATCH_SIZE = 1000


def env_enabled() -> bool:
    raw = os.getenv(ENV_ENABLED, "").strip().strip('"').strip("'")
    return bool(raw) and raw.lower() in TRUTHY


def batch_size() -> int:
    raw = os.getenv(ENV_BATCH_SIZE, "").strip()
    if raw:
        try:
            return max(1, int(raw))
        except ValueError:
            pass
    return DEFAULT_BATCH_SIZE


def fast_executemany_enabled() -> bool:
    raw = os.getenv(ENV_FAST_EXECUTEMANY, "true").strip().strip('"').strip("'")
    return raw.lower() in TRUTHY


def require_env_gate(action: str) -> None:
    settings.refresh_from_env()
    reload_env()
    if not env_enabled():
        print(
            f"{ENV_ENABLED} is missing or not 'true'. Failing safely — no Azure {action}.",
            file=sys.stderr,
        )
        raise SystemExit(1)


def connect_outbound_engine(*, fast_executemany: bool | None = None) -> Engine:
    """Connect with the existing project Azure config (SERVER/DATABASE/USERNAME/DRIVER)."""
    settings.refresh_from_env()
    reload_env()
    for key in ("SERVER", "DATABASE", "USERNAME"):
        if not os.getenv(key, "").strip():
            print(f"Azure config missing: {key} is required.", file=sys.stderr)
            raise SystemExit(1)

    use_fast = fast_executemany_enabled() if fast_executemany is None else fast_executemany
    print(f"  fast_executemany: {use_fast}")
    print(f"  batch_size: {batch_size()}")
    try:
        engine = create_engine(build_connection_url(), future=True, fast_executemany=use_fast)
        test_azure_connection_query(engine)
        print("Azure connection successful")
        return engine
    except Exception as exc:
        print(f"Azure connection failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
