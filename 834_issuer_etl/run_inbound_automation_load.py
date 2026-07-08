#!/usr/bin/env python3
"""
Inbound automation loader — Parser834 raw ingestion to Azure SQL.

Phase 1: dry-run only (discover, parse, enrich, Excel reports).
No Azure connection, no DDL, no database writes.

Examples:
    python run_inbound_automation_load.py --year 2025 --dry-run
    python run_inbound_automation_load.py --issuer 15105 --year 2025 --month 10 --dry-run
    python run_inbound_automation_load.py --all-years --dry-run
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from inbound_automation.cli import main

_IDE_DEFAULT_ARGS = ["--year", "2025", "--dry-run"]


if __name__ == "__main__":
    argv = _IDE_DEFAULT_ARGS if len(sys.argv) == 1 else None
    if argv is not None:
        print("No CLI arguments — using IDE defaults: --year 2025 --dry-run")
    raise SystemExit(main(argv))
