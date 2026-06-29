#!/usr/bin/env python3
"""
Chandra-style report-only runner — reads YEAR_FILTER / ISSUER_FILTER / MONTH_FILTER from .env.

Optional CLI overrides: --year, --issuer, --month, --force, --dry-run

Examples:
    python run_chandra_report_only.py --force
    python run_chandra_report_only.py --dry-run
    python run_chandra_report_only.py --issuer 15105 --year 2026 --force
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.chandra_report_only import run_chandra_report_only
from azure_reconciliation.safe_export import ExportErrors
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def _blank_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = str(value).strip()
    return stripped if stripped else None


def resolve_filters(args: argparse.Namespace) -> tuple[str, str | None, str | None]:
    """
    Resolve partition filters — CLI overrides .env when provided.

    Only reads YEAR_FILTER, ISSUER_FILTER, MONTH_FILTER from settings (.env).
    """
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
        description="Chandra-style reports only (filters from .env; no other .env flags)",
    )
    p.add_argument("--year", required=False, help="Override YEAR_FILTER from .env")
    p.add_argument("--issuer", required=False, help="Override ISSUER_FILTER from .env")
    p.add_argument("--month", required=False, help="Override MONTH_FILTER from .env")
    p.add_argument(
        "--force",
        action="store_true",
        help="Clear and overwrite outputs for the selected scope",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover and process without writing output files",
    )
    return p.parse_args()


def _print_banner(year: str, issuer: str | None, month: str | None, *, dry_run: bool) -> None:
    print("\nCHANDRA REPORT ONLY RUN")
    print(f"YEAR_FILTER={year}")
    print(f"ISSUER_FILTER={issuer or 'ALL'}")
    print(f"MONTH_FILTER={month or 'ALL'}")
    print("Prior-year benefit filter: FORCED ON")
    print("Azure: DISABLED")
    print("Legacy assets: DISABLED")
    print("Full exports: DISABLED")
    if dry_run:
        print("Mode: DRY RUN (no files written)")


def main() -> None:
    args = parse_args()

    try:
        year, issuer, month = resolve_filters(args)
    except RuntimeError as exc:
        logger.error("%s", exc)
        sys.exit(1)

    _print_banner(year, issuer, month, dry_run=args.dry_run)

    settings.apply_xml_only_business_mode(True)
    settings.ensure_dirs()

    errors = ExportErrors()
    try:
        result = run_chandra_report_only(
            year=year,
            issuer_filter=issuer,
            month_filter=month,
            force=args.force,
            dry_run=args.dry_run,
            export_errors=errors,
        )
    except RuntimeError as exc:
        logger.error("%s", exc)
        print(f"\nFAIL — {exc}")
        sys.exit(1)

    print(f"\n{result['final_status']}")
    print(f"  Output root        : {result['output_root']}")
    if not args.dry_run:
        print(f"  Run summary        : {result['run_summary']}")
    print(f"  Issuers discovered : {result['issuers_discovered']}")
    print(f"  Issuers processed  : {result['issuers_processed']}")
    print(f"  Months discovered  : {result['months_discovered']}")
    print(f"  Months processed   : {result['months_processed']}")
    print(f"  Months skipped     : {result['skipped_count']}")
    print(f"  Elapsed            : {result['elapsed_sec']:.1f}s")
    if errors.errors:
        print(f"  Export errors      : {len(errors.errors)}")
        sys.exit(1 if result["final_status"] == "FAIL" else 0)


if __name__ == "__main__":
    main()
