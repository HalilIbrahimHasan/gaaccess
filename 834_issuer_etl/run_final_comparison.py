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
from azure_reconciliation.debug_outputs import write_all_debug_outputs  # noqa: E402
from azure_reconciliation.final_comparison_engine import (  # noqa: E402
    print_final_result,
    run_final_comparison,
)
from azure_reconciliation.final_comparison_exporter import export_final_comparison  # noqa: E402
from azure_reconciliation.issuer_reports import generate_issuer_reports  # noqa: E402
from azure_reconciliation.lifecycle_engine import build_all_lifecycle_snapshots  # noqa: E402
from azure_reconciliation.safe_export import ExportErrors  # noqa: E402
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

    export_errors = ExportErrors()
    stats: dict = {"azure_requested": True, "skip_azure": False}

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
    stats["xml_raw_rows"] = len(xml_raw)
    stats["xml_lifecycle_rows"] = len(xml_lifecycle)

    logger.info("AZURE STAGE STARTED")
    try:
        engine, conn_meta = connect_azure(runner_name="run_final_comparison.py", strict=True)
    except Exception as exc:
        logger.error("Azure connection failed: %s", exc)
        sys.exit(1)
    stats["azure_available"] = True
    stats["connection"] = conn_meta

    comparison = run_final_comparison(
        engine,
        issuer=args.issuer,
        partitions=coverage.partitions,
        xml_raw=xml_raw,
        xml_lifecycle=xml_lifecycle,
    )
    comparison["azure_lifecycle_rows"] = 0
    comparison["azure_raw_rows"] = 0

    try:
        paths = export_final_comparison(comparison, issuer=args.issuer, export_errors=export_errors)
        stats["final_comparison_paths"] = paths
    except Exception as exc:
        export_errors.record(f"export_final_comparison: {exc}")

    print_final_result(comparison)

    winner = comparison.get("winner")
    if winner:
        stats["final_selected_table"] = winner.source_table
        stats["final_selected_strategy"] = winner.strategy_id
        stats["final_overall_accuracy"] = winner.overall_accuracy

    import pandas as pd  # noqa: E402
    strategy_scores = pd.DataFrame([
        {"strategy_id": r.strategy_id, "source_table": r.source_table,
         "overall_accuracy": r.overall_accuracy, **r.scores}
        for r in comparison.get("results", [])
    ])

    try:
        generate_issuer_reports(
            issuers=[args.issuer],
            partitions=coverage.partitions,
            xml_raw=xml_raw,
            xml_lifecycle=xml_lifecycle,
            azure_raw=pd.DataFrame(),
            azure_lifecycle=pd.DataFrame(),
            comparison_detail=pd.DataFrame(),
            comparison_summary=pd.DataFrame(),
            export_errors=export_errors,
        )
    except Exception as exc:
        export_errors.record(f"issuer_reports: {exc}")

    coverage_df = pd.DataFrame([
        {"issuer": p.issuer, "year": p.year, "month": p.month} for p in coverage.partitions
    ])
    write_all_debug_outputs(
        stats=stats,
        coverage_df=coverage_df,
        export_errors=export_errors,
        strategy_scores=strategy_scores,
        validation={"checks": [], "all_passed": winner is not None},
    )

    logger.info("Outputs: %s", stats.get("final_comparison_paths"))
    if winner is None and not comparison.get("results"):
        sys.exit(1)


if __name__ == "__main__":
    main()
