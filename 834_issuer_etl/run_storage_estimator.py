#!/usr/bin/env python3
"""Read-only storage estimator — no business report generation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.raw_db_storage_estimator import run_raw_db_storage_all_issuers
from azure_reconciliation.storage_estimator import run_storage_estimator
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def _blank_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = str(value).strip()
    return stripped if stripped else None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Storage estimator (read-only, fast)")
    p.add_argument("--issuer", help="Filter by issuer")
    p.add_argument("--year", help="Filter by year")
    p.add_argument("--month", help="Filter by month (comma-separated ok)")
    p.add_argument(
        "--all-issuers",
        action="store_true",
        help="Include all issuers under source_data for the selected year",
    )
    p.add_argument(
        "--raw-db-only",
        action="store_true",
        help="Estimate raw parsed 834 DB storage only (stg_834_records) — no canonical/reports",
    )
    return p.parse_args()


def _resolve_issuer_filter(args: argparse.Namespace) -> str | None:
    """Blank ISSUER_FILTER or --all-issuers => all issuers (None)."""
    if args.all_issuers:
        return None
    issuer = _blank_to_none(args.issuer) if args.issuer is not None else _blank_to_none(settings.issuer_filter)
    return issuer


def main() -> None:
    args = parse_args()
    settings.refresh_from_env()

    year = _blank_to_none(args.year) or _blank_to_none(settings.year_filter)
    month = _blank_to_none(args.month) if args.month is not None else _blank_to_none(settings.month_filter)
    issuer_filter = _resolve_issuer_filter(args)

    if not year:
        logger.error("YEAR_FILTER is required — set in .env or pass --year")
        sys.exit(1)

    settings.ensure_dirs()

    if args.raw_db_only:
        print("\nRAW 834 DB STORAGE — ALL ISSUERS")
        print(f"YEAR_FILTER={year}")
        print(f"ISSUER_FILTER={'ALL' if issuer_filter is None else issuer_filter}")
        print(f"MONTH_FILTER={month or 'ALL'}")
        print("Scope: stg_834_records parsed from source_data (all issuers)")
        print("No canonical, business-ready, reports, or diagnostics")

        result = run_raw_db_storage_all_issuers(
            year_filter=year,
            issuer_filter=issuer_filter,
            month_filter=month,
        )

        print("\nRaw 834 DB storage estimate complete.")
        print(f"  Summary XLSX : {result['summary_xlsx']}")
        print(f"  Summary CSV  : {result['summary_csv']}")
        print(f"  Markdown     : {result['summary_md']}")
        print(f"  Issuers      : {result['issuer_count']}")
        print(f"  Partitions   : {result['partition_rows']}")
        print(f"  Summary rows : {result['month_rows']} (includes TOTAL)")

        if result["partition_rows"] == 0:
            sys.exit(1)
        return

    settings.apply_cli_filters(args.issuer, args.year, args.month)

    result = run_storage_estimator(
        issuer_filter=issuer_filter,
        year_filter=year,
        month_filter=month,
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
