#!/usr/bin/env python3
"""
Final comparison engine — determine which Azure strategy reproduces XML business logic.

Runs after discovery (read-only). Does NOT modify SFTP, source_data, or XML parser.

Example:
    python run_final_comparison.py --issuer 13535
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.azure_client import azure_configured, connect_azure  # noqa: E402
from azure_reconciliation.final_comparison_engine import (  # noqa: E402
    print_final_result,
    run_final_comparison,
)
from azure_reconciliation.final_comparison_exporter import export_final_comparison  # noqa: E402
from azure_reconciliation.lifecycle_engine import build_all_lifecycle_snapshots  # noqa: E402
from azure_reconciliation.source_coverage import discover_source_coverage  # noqa: E402
from azure_reconciliation.xml_loader import load_xml_rows  # noqa: E402
from config.config import log_azure_startup_diagnostics, settings  # noqa: E402
from utils.logger import get_logger  # noqa: E402

logger = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Final Azure vs XML business comparison")
    parser.add_argument("--issuer", required=True, help="Issuer to compare")
    parser.add_argument("--parse-source", action="store_true", help="Parse source_data instead of staging")
    args = parser.parse_args()

    settings.refresh_from_env()
    log_azure_startup_diagnostics()
    settings.apply_cli_filters(args.issuer, None, None)
    settings.ensure_dirs()

    if not settings.azure_enabled:
        logger.error("ENABLE_AZURE must be true for final comparison")
        sys.exit(1)
    if not azure_configured():
        logger.error("Azure credentials missing in .env")
        sys.exit(1)

    coverage = discover_source_coverage(issuer_filter=args.issuer)
    if not coverage.partitions:
        logger.error("No source_data partitions for issuer %s", args.issuer)
        sys.exit(1)

    xml_raw = load_xml_rows(
        prefer_staging=not args.parse_source,
        issuer_filter=args.issuer,
    )
    xml_lifecycle = build_all_lifecycle_snapshots(xml_raw, coverage.partitions)

    logger.info("AZURE STAGE STARTED")
    engine, _ = connect_azure(runner_name="run_final_comparison.py", strict=True)

    comparison = run_final_comparison(
        engine,
        issuer=args.issuer,
        partitions=coverage.partitions,
        xml_raw=xml_raw,
        xml_lifecycle=xml_lifecycle,
    )

    paths = export_final_comparison(comparison, issuer=args.issuer)
    print_final_result(comparison)

    logger.info("Outputs: %s", paths)
    winner = comparison.get("winner")
    if winner is None or winner.overall_accuracy < 90.0:
        sys.exit(1)


if __name__ == "__main__":
    main()
