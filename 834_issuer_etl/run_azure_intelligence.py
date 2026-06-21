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
from config.config import settings  # noqa: E402
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
    args = parse_args()
    settings.apply_cli_filters(args.issuer, args.year, args.month)
    settings.ensure_dirs()

    if not settings.azure_enabled and not args.skip_azure:
        logger.info("ENABLE_AZURE=false — running XML-only intelligence (metadata/lifecycle)")
        args.skip_azure = True

    stats = run_intelligence_pipeline(
        issuer_filter=settings.issuer_filter,
        year_filter=settings.year_filter,
        month_filter=settings.month_filter,
        prefer_staging=not args.parse_source,
        skip_azure=args.skip_azure,
    )

    validation = stats.get("validation", {})
    if validation and not validation.get("all_passed") and settings.azure_enabled and not args.skip_azure:
        sys.exit(1)


if __name__ == "__main__":
    main()
