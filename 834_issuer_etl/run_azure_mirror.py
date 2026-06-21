#!/usr/bin/env python3
"""
Generate Azure mirror reports under assets/{issuer}/azurevs/.

Standalone entry point — does not run the XML pipeline.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from azure_reconciliation.azure_mirror.exporter import export_azure_mirror_reports  # noqa: E402
from utils.logger import get_logger  # noqa: E402

logger = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Azure mirror reports under assets/{issuer}/azurevs/")
    parser.add_argument("--issuer", help="Filter to one issuer")
    parser.add_argument("--year", help="Filter to one year")
    parser.add_argument("--month", help="Filter to one month")
    parser.add_argument(
        "--no-discovery", action="store_true",
        help="Skip Azure logic discovery (active coverage reports only)",
    )
    args = parser.parse_args()

    stats = export_azure_mirror_reports(
        issuer_filter=args.issuer,
        year_filter=args.year,
        month_filter=args.month,
        run_discovery=not args.no_discovery,
    )
    logger.info("Done: %s", stats)


if __name__ == "__main__":
    main()
