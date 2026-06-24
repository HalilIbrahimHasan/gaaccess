#!/usr/bin/env python3
"""
Assemble a clean business review package for Hari / Chandra / stakeholders.

Orchestrates existing export and investigation runners, then copies outputs into
outputs/business_review/ without modifying original files.

Examples:
    python run_business_review_package.py --issuer 15105 --year 2026 --parse-source
    python run_business_review_package.py --issuer 15105 --parse-source
    python run_business_review_package.py --parse-source
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.business_review_package import run_business_review_package
from azure_reconciliation.safe_export import ExportErrors
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Assemble business review package from existing exports")
    p.add_argument("--issuer", help="Process only this issuer")
    p.add_argument("--year", help="Process only this year")
    p.add_argument("--month", help="Copy only this month's enrollment_summary (default: all months)")
    p.add_argument(
        "--parse-source",
        action="store_true",
        help="Parse XML from source_data directly (skip staging SQLite)",
    )
    p.add_argument("--expected-confirm", type=int, default=None)
    p.add_argument("--expected-cancel", type=int, default=None)
    p.add_argument("--expected-term", type=int, default=None)
    p.add_argument(
        "--gap-month",
        default="01",
        help="Business month for optional final gap investigation (default: 01)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    settings.refresh_from_env()
    settings.apply_xml_only_business_mode(True)
    settings.ensure_dirs()

    export_errors = ExportErrors()
    try:
        result = run_business_review_package(
            issuer_filter=args.issuer,
            year_filter=args.year,
            month_filter=args.month,
            parse_source=args.parse_source,
            expected_confirm=args.expected_confirm,
            expected_cancel=args.expected_cancel,
            expected_term=args.expected_term,
            gap_month=args.gap_month,
            export_errors=export_errors,
        )
    except RuntimeError as exc:
        logger.error("%s", exc)
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("BUSINESS REVIEW PACKAGE COMPLETE")
    logger.info("Output root    : %s", result.get("output_root"))
    logger.info("README         : %s", result.get("readme"))
    logger.info("Index HTML     : %s", result.get("index_html"))
    logger.info("Issuer/years   : %d", result.get("issuer_years", 0))
    for pkg in result.get("packages", []):
        copied = sum(1 for c in pkg.copies if c.copied)
        logger.info(
            "  %s/%s — %d file(s) copied, %d missing",
            pkg.issuer, pkg.year, copied, len(pkg.missing),
        )
    logger.info("=" * 60)

    if export_errors.errors:
        export_errors.write_debug_file()
        logger.warning("%d orchestration warning(s) — see export_errors.txt", len(export_errors.errors))


if __name__ == "__main__":
    main()
