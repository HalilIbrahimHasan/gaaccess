#!/usr/bin/env python3
"""Test Azure SQL connection and print diagnostics."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from azure_reconciliation.azure_client import (  # noqa: E402
    azure_configured,
    get_connection_meta,
    passwordless_connect_candidates,
    verify_azure_connection,
    _azure_settings,
)
from utils.logger import get_logger  # noqa: E402

logger = get_logger(__name__)


def main() -> None:
    meta = get_connection_meta()
    print("=== Azure Connection Diagnostics ===")
    print(f"configured (SERVER+DATABASE): {azure_configured()}")
    for k, v in meta.items():
        print(f"  {k}: {v}")

    if not azure_configured():
        print("\nSet SERVER and DATABASE in .env (PASSWORD not required).")
        sys.exit(1)

    cfg = _azure_settings()
    if not cfg["password"]:
        print("\nPasswordless modes to try:")
        for mode, _ in passwordless_connect_candidates(cfg):
            print(f"  - {mode}")

    engine, result = verify_azure_connection()
    print("\n=== Result ===")
    for k, v in result.items():
        print(f"  {k}: {v}")

    if engine is None:
        sys.exit(2)
    print("\nOK — Azure connection successful")


if __name__ == "__main__":
    main()
