#!/usr/bin/env python3
"""
Forensic SQL validation — evidence-only Azure vs XML diagnostics.

Does NOT modify business logic. Read-only SELECT against dbo.834_Inbound_test.

Examples:
    python run_sql_validation.py --issuer 37001 --parse-source
    python run_sql_validation.py --parse-source
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.azure_client import azure_configured, connect_azure
from azure_reconciliation.partition_discovery import discover_partitions
from azure_reconciliation.sql_forensic_validation import run_sql_forensic_validation
from azure_reconciliation.xml_loader import load_xml_rows
from config.config import log_azure_startup_diagnostics, settings
from utils.logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Forensic SQL validation (read-only)")
    parser.add_argument("--issuer", default=None, help="Issuer from source_data (default: all discovered)")
    parser.add_argument(
        "--parse-source",
        action="store_true",
        help="Parse source_data directly instead of staging SQLite",
    )
    args = parser.parse_args()

    settings.refresh_from_env()
    settings.apply_fast_mode(True)
    log_azure_startup_diagnostics()
    settings.ensure_dirs()

    partitions = discover_partitions(settings.source_data_path)
    issuers = [args.issuer] if args.issuer else sorted({p.issuer for p in partitions})
    if not issuers:
        logger.error("No issuers discovered in source_data")
        sys.exit(1)

    if not azure_configured():
        logger.error("Azure not configured — set AZURE_SQL_SERVER, AZURE_SQL_DATABASE, AZURE_SQL_USERNAME")
        sys.exit(1)

    engine, meta = connect_azure(runner_name="sql_validation", strict=True)
    if engine is None:
        logger.error("Azure connection failed: %s", meta.get("error"))
        sys.exit(1)

    all_paths: dict[str, dict[str, str]] = {}
    for issuer in issuers:
        logger.info("=== SQL forensic validation issuer=%s ===", issuer)
        xml_raw = load_xml_rows(
            prefer_staging=not args.parse_source,
            issuer_filter=issuer,
        )
        if xml_raw.empty:
            logger.warning("No XML rows for issuer %s — continuing with Azure-only diagnostics", issuer)

        issuer_parts = [p for p in partitions if p.issuer == issuer]
        paths = run_sql_forensic_validation(
            engine,
            issuer=issuer,
            xml_raw=xml_raw,
            partitions=issuer_parts,
        )
        all_paths[issuer] = paths
        for name, path in paths.items():
            logger.info("  %s → %s", name, path)

    print("\nSQL forensic validation complete.")
    for issuer, paths in all_paths.items():
        print(f"\nIssuer {issuer}:")
        for name, path in paths.items():
            print(f"  {name}: {path}")


if __name__ == "__main__":
    main()
