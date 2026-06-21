#!/usr/bin/env python3
"""Run Azure logic discovery only (no active coverage report regeneration)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from azure_reconciliation.azure_mirror.discovery.runner import export_azure_logic_discovery  # noqa: E402
from utils.logger import get_logger  # noqa: E402

logger = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Azure logic discovery vs XML summaries")
    parser.add_argument("--issuer", help="Filter to one issuer")
    parser.add_argument("--year", help="Filter to one year")
    parser.add_argument("--month", help="Filter to one month")
    args = parser.parse_args()

    stats = export_azure_logic_discovery(
        issuer_filter=args.issuer,
        year_filter=args.year,
        month_filter=args.month,
    )
    logger.info("Discovery complete: %s", stats)


if __name__ == "__main__":
    main()
