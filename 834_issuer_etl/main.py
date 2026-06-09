#!/usr/bin/env python3
"""
834 Issuer ETL — full local pipeline.

Examples:
    python main.py
    python main.py --issuer Sigma
    python main.py --issuer 86637 --year 2026 --month 02
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.config import settings  # noqa: E402
from pipeline.orchestrator import Pipeline  # noqa: E402
from utils.logger import get_logger  # noqa: E402

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="834 XML ETL — full pipeline")
    p.add_argument("--issuer", help="Filter by issuer folder name")
    p.add_argument("--year", help="Filter by 4-digit year")
    p.add_argument("--month", help="Filter by month (1-12 or 01-12)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    settings.apply_cli_filters(args.issuer, args.year, args.month)
    settings.ensure_dirs()

    logger.info("PROJECT_ROOT : %s", settings.project_root)
    logger.info("SOURCE_DATA  : %s", settings.source_data_path)
    logger.info("DATABASE     : %s", settings.database_path)
    logger.info("MODE         : %s", settings.processing_mode)
    if settings.issuer_filter:
        logger.info("ISSUER FILTER: %s", settings.issuer_filter)

    pipeline = Pipeline()
    try:
        stats = pipeline.run_full(settings.issuer_filter)
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETE")
        logger.info("Files discovered : %d", stats["files_discovered"])
        logger.info("Files processed  : %d", stats["files_processed"])
        logger.info("Files skipped    : %d", stats["files_skipped"])
        logger.info("Files failed     : %d", stats["files_failed"])
        logger.info("Records loaded   : %d", stats["records_loaded"])
        logger.info("Reports          : %s", settings.reports_path)
        logger.info("=" * 60)
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
