#!/usr/bin/env python3
"""Read-only storage estimator — no business report generation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.storage_estimator import run_storage_estimator
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Storage estimator (read-only, fast)")
    p.add_argument("--issuer", help="Filter by issuer")
    p.add_argument("--year", help="Filter by year")
    p.add_argument("--month", help="Filter by month")
    p.add_argument("--all-issuers", action="store_true", help="All issuers for --year")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    settings.refresh_from_env()
    settings.apply_cli_filters(args.issuer, args.year, args.month)
    settings.ensure_dirs()

    result = run_storage_estimator(
        issuer_filter=None if args.all_issuers else args.issuer or settings.issuer_filter,
        year_filter=args.year or settings.year_filter,
        month_filter=args.month or settings.month_filter,
        all_issuers=args.all_issuers,
    )

    print("\nStorage estimate complete.")
    print(f"  Output root  : {result['output_root']}")
    print(f"  Summary      : {result['summary_xlsx']}")
    print(f"  Snowflake est: {result['snowflake_xlsx']}")
    print(f"  By partition : {result['by_part_xlsx']}")
    print(f"  Markdown     : {result['summary_md']}")
    print(f"  Partitions   : {result['partitions']}")


if __name__ == "__main__":
    main()
