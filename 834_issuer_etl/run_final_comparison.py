#!/usr/bin/env python3
"""
Final comparison — FAST_MODE by default (834_Inbound_test / Strategy C).

Examples:
    python run_final_comparison.py --issuer 13535 --fast
    python run_final_comparison.py --issuer 13535 --full-discovery
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
from azure_reconciliation.safe_export import ExportErrors, write_csv_fallback  # noqa: E402
from azure_reconciliation.source_coverage import discover_source_coverage  # noqa: E402
from azure_reconciliation.xml_loader import load_xml_rows  # noqa: E402
from config.config import log_azure_startup_diagnostics, settings  # noqa: E402
from utils.logger import get_logger  # noqa: E402

logger = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Final Azure vs XML business comparison")
    parser.add_argument("--issuer", required=True, help="Issuer to compare")
    parser.add_argument("--parse-source", action="store_true", help="Parse source_data instead of staging")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--fast", action="store_true", help="FAST_MODE — fixed 834_Inbound_test / Strategy C (default)")
    mode.add_argument("--full-discovery", action="store_true", help="Scan all Azure tables (slow)")
    args = parser.parse_args()

    settings.refresh_from_env()
    if args.full_discovery:
        settings.enable_full_discovery = True
        settings.use_fixed_azure_candidate = False
        settings.fast_mode = False
    else:
        settings.apply_fast_mode(True)

    log_azure_startup_diagnostics()
    settings.apply_cli_filters(args.issuer, None, None)
    settings.ensure_dirs()

    export_errors = ExportErrors()
    stats: dict = {
        "azure_requested": True,
        "skip_azure": False,
        "fast_mode": settings.fast_mode,
        "enable_full_discovery": settings.enable_full_discovery,
    }

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

    # CSV fallback first — before any export that might fail
    write_csv_fallback("xml_raw", xml_raw, export_errors=export_errors)
    write_csv_fallback("xml_lifecycle", xml_lifecycle, export_errors=export_errors)

    logger.info("AZURE STAGE STARTED (fast_mode=%s)", settings.fast_mode)
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
    stats["azure_raw_rows"] = comparison.get("azure_raw_rows", 0)
    stats["match_count"] = comparison.get("match_count", 0)

    paths: dict[str, str] = {}
    try:
        paths = export_final_comparison(comparison, issuer=args.issuer, export_errors=export_errors)
        stats["final_comparison_paths"] = paths
    except Exception as exc:
        export_errors.record(f"export_final_comparison: {exc}")

    comparison["final_comparison_paths"] = paths
    comparison["output_paths"] = paths
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

    az_raw = pd.DataFrame()
    try:
        generate_issuer_reports(
            issuers=[args.issuer],
            partitions=coverage.partitions,
            xml_raw=xml_raw,
            xml_lifecycle=xml_lifecycle,
            azure_raw=az_raw,
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

    logger.info("Outputs: %s", paths)
    if winner is None and not comparison.get("results"):
        sys.exit(1)


if __name__ == "__main__":
    main()
