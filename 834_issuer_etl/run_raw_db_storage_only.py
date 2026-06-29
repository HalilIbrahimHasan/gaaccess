#!/usr/bin/env python3
"""
Raw 834 DB storage estimator — parsed staging table footprint only.

Reads only YEAR_FILTER, ISSUER_FILTER, MONTH_FILTER from .env.

Does NOT run business reports, canonical, lifecycle, Model H, Azure, or exports.

Example:
    python run_raw_db_storage_only.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.raw_db_storage_estimator import run_raw_db_storage_only
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def _blank_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = str(value).strip()
    return stripped if stripped else None


def resolve_filters(args: argparse.Namespace) -> tuple[str, str | None, str | None]:
    """Resolve filters — CLI overrides .env when provided."""
    year = _blank_to_none(args.year) or _blank_to_none(settings.year_filter)
    issuer = _blank_to_none(args.issuer) if args.issuer is not None else _blank_to_none(settings.issuer_filter)
    month = _blank_to_none(args.month) if args.month is not None else _blank_to_none(settings.month_filter)
    if not year:
        raise RuntimeError(
            "YEAR_FILTER is required — set YEAR_FILTER in .env or pass --year"
        )
    return year, issuer, month


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Raw 834 DB storage estimate only (filters from .env)",
    )
    p.add_argument("--year", help="Override YEAR_FILTER from .env")
    p.add_argument("--issuer", help="Override ISSUER_FILTER from .env")
    p.add_argument("--month", help="Override MONTH_FILTER from .env (comma-separated ok)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    settings.refresh_from_env()

    try:
        year, issuer, month = resolve_filters(args)
    except RuntimeError as exc:
        logger.error("%s", exc)
        sys.exit(1)

    print("\nRAW 834 DB STORAGE ESTIMATE")
    print(f"YEAR_FILTER={year}")
    print(f"ISSUER_FILTER={issuer or 'ALL'}")
    print(f"MONTH_FILTER={month or 'ALL'}")
    print("Scope: stg_834_records (parsed raw 834 XML only)")
    print("No business reports, canonical, lifecycle, or Model H")

    settings.ensure_dirs()

    result = run_raw_db_storage_only(
        year_filter=year,
        issuer_filter=issuer,
        month_filter=month,
    )

    print("\nRaw 834 DB storage estimate complete.")
    print(f"  Summary XLSX : {result['summary_xlsx']}")
    print(f"  Summary CSV  : {result['summary_csv']}")
    print(f"  Month rows   : {result['month_rows']}")
    print(f"  Partitions   : {result['partition_rows']}")

    if result["month_rows"] == 0:
        print("\nNo partitions matched filters — summary files contain headers only.")
        sys.exit(1)


if __name__ == "__main__":
    main()
