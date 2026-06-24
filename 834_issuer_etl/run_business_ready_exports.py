#!/usr/bin/env python3
"""
Business-ready record exports — Model H input validation.

Examples:
    python run_business_ready_exports.py --issuer 15105 --year 2026 --parse-source
    python run_business_ready_exports.py --issuer 15105 --parse-source
    python run_business_ready_exports.py --parse-source
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.business_ready_exports import run_business_ready_exports  # noqa: E402
from azure_reconciliation.safe_export import ExportErrors  # noqa: E402
from config.config import settings  # noqa: E402
from utils.logger import get_logger  # noqa: E402

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Export business-ready records (Model H input) by issuer/year",
    )
    p.add_argument("--issuer", help="Export only this issuer folder")
    p.add_argument("--year", help="Export only this year")
    p.add_argument(
        "--parse-source",
        action="store_true",
        help="Parse XML from source_data directly (skip staging SQLite)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    settings.refresh_from_env()
    settings.ensure_dirs()

    export_errors = ExportErrors()
    stats = run_business_ready_exports(
        issuer_filter=args.issuer,
        year_filter=args.year,
        parse_source=args.parse_source,
        export_errors=export_errors,
    )

    logger.info("=" * 60)
    logger.info("BUSINESS READY EXPORT COMPLETE")
    logger.info("Issuer/years exported : %d", stats.get("issuer_years", 0))
    logger.info("Output root           : %s", stats.get("output_root"))
    for r in stats.get("results", []):
        logger.info(
            "  %s/%s — %d record(s) → %s (summary: %s)",
            r.issuer, r.year, r.record_count, r.excel_path, r.summary_path,
        )
    logger.info("=" * 60)

    if export_errors.errors:
        debug_path = export_errors.write_debug_file()
        logger.warning("Export errors recorded: %s", debug_path)


if __name__ == "__main__":
    main()
