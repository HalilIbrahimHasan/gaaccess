#!/usr/bin/env python3
"""Fast business reporting — lightweight Excel/CSV without legacy assets pipeline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.fast_business_reports import run_fast_business_reports
from azure_reconciliation.safe_export import ExportErrors
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fast business reports (no legacy assets pipeline)")
    p.add_argument("--issuer", help="Filter by issuer")
    p.add_argument("--year", help="Filter by year")
    p.add_argument("--month", help="Filter by month (01-12)")
    p.add_argument("--all-issuers", action="store_true", help="Process all issuers for --year")
    p.add_argument("--parse-source", action="store_true", help="Parse XML from source_data directly")
    p.add_argument("--force", action="store_true", help="Regenerate even if outputs exist")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    settings.refresh_from_env()
    settings.apply_fast_business_report_mode(True)
    settings.apply_cli_filters(args.issuer, args.year, args.month)
    settings.ensure_dirs()

    errors = ExportErrors()
    try:
        result = run_fast_business_reports(
            issuer_filter=None if args.all_issuers else args.issuer or settings.issuer_filter,
            year_filter=args.year or settings.year_filter,
            month_filter=args.month or settings.month_filter,
            all_issuers=args.all_issuers,
            parse_source=args.parse_source,
            force=args.force,
            export_errors=errors,
        )
    except RuntimeError as exc:
        logger.error("%s", exc)
        sys.exit(1)

    print("\nFast business reports complete.")
    print(f"  Output root : {result['output_root']}")
    print(f"  Year rollup : {result['year_rollup_dir']}")
    print(f"  Index       : {result['index_html']}")
    print(f"  Issuer/years: {result['issuer_years']}")
    print(f"  Elapsed     : {result['elapsed_sec']:.1f}s")
    if errors.errors:
        print(f"  Export errors: {len(errors.errors)} (see outputs/debug/export_errors.txt)")


if __name__ == "__main__":
    main()
