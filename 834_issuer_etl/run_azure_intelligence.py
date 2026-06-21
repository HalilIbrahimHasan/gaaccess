#!/usr/bin/env python3
"""
Unified Azure intelligence runner — full production pipeline.

Extension only. Does NOT modify SFTP, source_data, or XML parser.

Examples:
    python run_azure_intelligence.py --issuer 13535
    python run_azure_intelligence.py --skip-azure
    python run_azure_intelligence.py --parse-source
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.intelligence_pipeline import run_intelligence_pipeline  # noqa: E402
from config.config import log_azure_startup_diagnostics, settings  # noqa: E402
from utils.logger import get_logger  # noqa: E402

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Azure intelligence — full production pipeline")
    p.add_argument("--issuer", help="Filter by issuer")
    p.add_argument("--year", help="Filter by year")
    p.add_argument("--month", help="Filter by month")
    p.add_argument("--skip-azure", action="store_true", help="XML lifecycle + metadata only")
    p.add_argument("--parse-source", action="store_true", help="Parse source_data instead of staging SQLite")
    return p.parse_args()


def main() -> None:
    settings.refresh_from_env()
    log_azure_startup_diagnostics()

    args = parse_args()
    settings.apply_cli_filters(args.issuer, args.year, args.month)
    settings.ensure_dirs()

    skip_azure = args.skip_azure
    if not settings.azure_enabled:
        if not skip_azure:
            logger.info("ENABLE_AZURE is false — running XML-only intelligence")
        skip_azure = True

    logger.info(
        "Azure execution: azure_enabled=%s skip_azure=%s (CLI --skip-azure=%s)",
        settings.azure_enabled,
        skip_azure,
        args.skip_azure,
    )

    try:
        stats = run_intelligence_pipeline(
            issuer_filter=settings.issuer_filter,
            year_filter=settings.year_filter,
            month_filter=settings.month_filter,
            prefer_staging=not args.parse_source,
            skip_azure=skip_azure,
        )
    except RuntimeError as exc:
        logger.error("%s", exc)
        sys.exit(1)

    # Exit 1 only for fatal failures (connection, no source_data) — not low accuracy or export errors
    if stats.get("azure_requested") and not stats.get("azure_available"):
        sys.exit(1)


if __name__ == "__main__":
    main()
