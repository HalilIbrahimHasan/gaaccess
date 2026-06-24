#!/usr/bin/env python3
"""
Full raw + cleaned data exports for manual issuer/year review.

Examples:
    python run_full_data_exports.py --issuer 15105 --year 2026 --parse-source
    python run_full_data_exports.py --issuer 15105 --parse-source
    python run_full_data_exports.py --parse-source
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.full_data_exports import run_full_data_exports  # noqa: E402
from azure_reconciliation.safe_export import ExportErrors  # noqa: E402
from config.config import settings  # noqa: E402
from utils.logger import get_logger  # noqa: E402

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Full raw + cleaned Excel exports by issuer/year")
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
    stats = run_full_data_exports(
        issuer_filter=args.issuer,
        year_filter=args.year,
        parse_source=args.parse_source,
        export_errors=export_errors,
    )

    logger.info("=" * 60)
    logger.info("FULL DATA EXPORT COMPLETE")
    logger.info("Issuer/years exported : %d", stats.get("issuer_years", 0))
    logger.info("Output root           : %s", stats.get("output_root"))
    logger.info("Summary workbook      : %s", stats.get("summary_path"))
    logger.info("Index HTML            : %s", settings.outputs_path / "full_data_exports" / "index.html")
    logger.info("=" * 60)

    if export_errors.errors:
        debug_path = export_errors.write_debug_file()
        logger.warning("Export errors recorded: %s", debug_path)


if __name__ == "__main__":
    main()
