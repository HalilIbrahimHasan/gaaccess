#!/usr/bin/env python3
"""Test Azure SQL connection (ActiveDirectoryInteractive)."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from azure_reconciliation.azure_client import (  # noqa: E402
    AUTH_MODE,
    azure_configured,
    build_odbc_connect_string,
    get_connection_meta,
    verify_azure_connection,
)


def main() -> None:
    print("=== Azure Connection Diagnostics ===")
    print(f"configured (SERVER+DATABASE+USERNAME): {azure_configured()}")
    for k, v in get_connection_meta().items():
        print(f"  {k}: {v}")

    if not azure_configured():
        print("\nSet SERVER, DATABASE, and USERNAME in .env")
        sys.exit(1)

    cs = build_odbc_connect_string()
    print(f"\nConnection string pattern:\n  {cs[:cs.index('UID=')]}UID=***;Authentication=...")

    engine, result = verify_azure_connection()
    print("\n=== Result ===")
    for k, v in result.items():
        print(f"  {k}: {v}")

    if engine is None:
        sys.exit(2)
    print(f"\nOK — Azure connection mode: {AUTH_MODE}")


if __name__ == "__main__":
    main()
