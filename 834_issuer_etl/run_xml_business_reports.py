#!/usr/bin/env python3
"""
XML-only Chandra-like business reporting — no Azure connection.

Examples:
    python run_xml_business_reports.py --parse-source
    python run_xml_business_reports.py --issuer 15105 --parse-source
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.safe_export import ExportErrors
from azure_reconciliation.xml_business_reports import run_xml_business_reporting
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="XML-only Model H business reports (no Azure)")
    parser.add_argument("--issuer", default=None, help="Process one issuer (default: all in source_data)")
    parser.add_argument(
        "--parse-source",
        action="store_true",
        help="Parse source_data directly instead of staging SQLite",
    )
    args = parser.parse_args()

    settings.refresh_from_env()
    settings.apply_xml_only_business_mode(True)
    settings.apply_cli_filters(args.issuer, None, None)
    settings.ensure_dirs()

    export_errors = ExportErrors()
    try:
        result = run_xml_business_reporting(
            issuer=args.issuer,
            parse_source=args.parse_source,
            export_errors=export_errors,
        )
    except RuntimeError as exc:
        logger.error("%s", exc)
        sys.exit(1)

    print("\nXML-only business reporting complete.")
    print(f"  Issuers: {', '.join(result['issuers'])}")
    print(f"  Output:  {result['output_root']}")
    print(f"  Assets:  {result.get('assets_root', '')}")
    print(f"  SQLite:  {result['sqlite']}")
    print(f"  Model H groups: {result['model_h_groups']}")

    if export_errors.errors:
        logger.warning("Export warnings: %s", export_errors.errors)


if __name__ == "__main__":
    main()
