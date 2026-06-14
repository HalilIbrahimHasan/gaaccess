#!/usr/bin/env python3
"""
834 Issuer ETL — full local pipeline.

Processes real issuer data from source_data/{issuer}/{year}/{month}/*.xml

Examples:
    python main.py
    python main.py --issuer 86637 --year 2026 --month 02
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.config import env_diagnostics, settings  # noqa: E402
from ingestion.sftp_filters import filters_from_settings, format_filter_display  # noqa: E402
from pipeline.orchestrator import Pipeline  # noqa: E402
from utils.logger import get_logger  # noqa: E402

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="834 XML ETL — full pipeline")
    p.add_argument("--issuer", help="Filter by 5-digit issuer folder")
    p.add_argument("--year", help="Filter by 4-digit year")
    p.add_argument("--month", help="Filter by month (1-12 or 01-12)")
    p.add_argument(
        "--no-clean",
        action="store_true",
        help="Keep previous run outputs (not recommended)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    settings.apply_cli_filters(args.issuer, args.year, args.month)
    if args.no_clean:
        settings.clean_on_start = False

    env_info = env_diagnostics()
    logger.info("ENV FILE     : %s", env_info["env_file"])
    logger.info("ENV EXISTS   : %s", env_info["env_file_exists"])
    logger.info("ENV LOADED   : %s", env_info["env_loaded"])
    logger.info(
        "PROCESSING_MODE (raw): %s",
        env_info["processing_mode_raw"] if env_info["processing_mode_raw"] else "(not set — default local)",
    )
    logger.info("PROJECT_ROOT : %s", settings.project_root)
    logger.info("SOURCE_DATA  : %s", settings.source_data_path)
    logger.info("ASSETS       : %s", settings.assets_path)
    logger.info("DATABASE     : %s", settings.database_path)
    logger.info("REPORTS      : %s", settings.reports_path)
    logger.info("MODE         : %s", settings.processing_mode)
    logger.info("SFTP AUDIT   : %s", settings.sftp_audit_only)
    issuer_allow, year_allow, month_allow = filters_from_settings(settings)
    logger.info("Effective filters:")
    logger.info("  issuers=%s", format_filter_display(issuer_allow))
    logger.info("  years=%s", format_filter_display(year_allow))
    logger.info("  months=%s", format_filter_display(month_allow))
    logger.info("CLEAN START  : %s", settings.clean_on_start)

    if settings.sftp_audit_only:
        if settings.processing_mode != "sftp":
            logger.error("SFTP_AUDIT_ONLY requires PROCESSING_MODE=sftp")
            sys.exit(1)
        settings.ensure_dirs()
        from connectors.sftp_connector import SFTPSourceConnector  # noqa: E402

        SFTPSourceConnector().sync()
        logger.info("=" * 60)
        logger.info("SFTP AUDIT COMPLETE (no download, no parser, no reconciliation)")
        logger.info("Summary CSV : %s", settings.reports_path / "sftp_ingestion_summary.csv")
        logger.info("Summary XLSX: %s", settings.reports_path / "sftp_ingestion_summary.xlsx")
        logger.info("=" * 60)
        return

    pipeline = Pipeline()
    try:
        stats = pipeline.run_full(settings.issuer_filter)
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETE")
        logger.info("Files discovered    : %d", stats["files_discovered"])
        logger.info("Files processed     : %d", stats["files_processed"])
        logger.info("Files skipped       : %d", stats["files_skipped"])
        logger.info("Files failed        : %d", stats["files_failed"])
        logger.info("Records loaded      : %d", stats["records_loaded"])
        logger.info("Asset partitions    : %d", stats.get("asset_partitions", 0))
        logger.info("Asset rollups       : %d", stats.get("asset_rollups", 0))
        logger.info("Reports             : %s", settings.reports_path)
        logger.info("Assets              : %s", settings.assets_path)
        logger.info("=" * 60)
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
