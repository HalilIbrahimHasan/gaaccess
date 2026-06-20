#!/usr/bin/env python3
"""
Azure vs XML enrollment reconciliation — standalone runner.

Does NOT modify the existing main.py pipeline.
Reads XML scope from source_data/ only.

Examples:
    python run_azure_reconciliation.py
    python run_azure_reconciliation.py --issuer 13535 --year 2026 --month 02
    python run_azure_reconciliation.py --skip-azure   # column mapping + XML only
    python run_azure_reconciliation.py --parse-source  # force parse source_data, ignore staging
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.runner import run_reconciliation  # noqa: E402
from config.config import settings  # noqa: E402
from utils.logger import get_logger  # noqa: E402

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Azure vs XML enrollment reconciliation")
    p.add_argument("--issuer", help="Filter by issuer (5-digit)")
    p.add_argument("--year", help="Filter by year")
    p.add_argument("--month", help="Filter by month")
    p.add_argument(
        "--skip-azure",
        action="store_true",
        help="Skip Azure queries (XML + column mapping only)",
    )
    p.add_argument(
        "--parse-source",
        action="store_true",
        help="Parse source_data directly instead of reading staging SQLite",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    settings.apply_cli_filters(args.issuer, args.year, args.month)

    logger.info("=" * 60)
    logger.info("AZURE vs XML RECONCILIATION")
    logger.info("source_data : %s", settings.source_data_path)
    logger.info("outputs     : %s", settings.project_root / "outputs" / "reconciliation")
    logger.info("=" * 60)

    stats = run_reconciliation(
        issuer_filter=settings.issuer_filter,
        year_filter=settings.year_filter,
        month_filter=settings.month_filter,
        prefer_staging=not args.parse_source,
        skip_azure=args.skip_azure,
    )

    logger.info("=" * 60)
    logger.info("RECONCILIATION COMPLETE")
    for key, val in stats.items():
        logger.info("  %s: %s", key, val)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
