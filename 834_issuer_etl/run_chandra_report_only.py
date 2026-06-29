#!/usr/bin/env python3
"""
Chandra-style report-only runner — independent, no .env feature flags.

Examples:
    python run_chandra_report_only.py --year 2026
    python run_chandra_report_only.py --year 2025
    python run_chandra_report_only.py --issuer 15105 --year 2026
    python run_chandra_report_only.py --issuer 15105 --year 2026 --month 01
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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Chandra-style reports only (no legacy pipelines, no .env flags)",
    )
    p.add_argument("--year", required=True, help="Reporting / partition year (e.g. 2026)")
    p.add_argument("--issuer", help="Optional 5-digit issuer filter")
    p.add_argument("--month", help="Optional month filter (01-12)")
    p.add_argument(
        "--force",
        action="store_true",
        help="Overwrite outputs (default: always clears selected scope before write)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    # Do NOT call settings.refresh_from_env() for feature flags — runner is self-contained.
    # Only use source_data path from settings.
    settings.apply_xml_only_business_mode(True)
    settings.ensure_dirs()

    errors = ExportErrors()
    try:
        result = run_chandra_report_only(
            year=str(args.year),
            issuer=args.issuer,
            month=args.month,
            force=args.force,
            export_errors=errors,
        )
    except RuntimeError as exc:
        logger.error("%s", exc)
        sys.exit(1)

    print("\nChandra report only complete.")
    print(f"  Output root   : {result['output_root']}")
    print(f"  Run summary   : {result['run_summary']}")
    print(f"  Issuer/years  : {result['issuer_years']}")
    print(f"  Months logged : {result['month_status_count']}")
    print(f"  Skipped       : {result['skipped_count']}")
    print(f"  Elapsed       : {result['elapsed_sec']:.1f}s")
    if errors.errors:
        print(f"  Export errors : {len(errors.errors)}")


if __name__ == "__main__":
    main()
