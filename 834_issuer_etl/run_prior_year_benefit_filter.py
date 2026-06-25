#!/usr/bin/env python3
"""
Prior-year benefit effective filter — filtered comparison exports.

Does not modify unfiltered pipeline outputs. Requires existing full_data_exports
and business_ready_exports (or run via run_business_review_package with filter enabled).

Examples:
    python run_prior_year_benefit_filter.py --issuer 15105 --year 2026
    python run_prior_year_benefit_filter.py --issuer 15105
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.full_data_exports import discover_issuer_year_pairs
from azure_reconciliation.prior_year_benefit_filter import (
    resolve_reporting_year,
    run_prior_year_benefit_filter_batch,
)
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Apply prior-year benefit effective filter to exported raw/business-ready data",
    )
    p.add_argument("--issuer", help="Filter only this issuer")
    p.add_argument("--year", help="Partition year and default reporting year")
    p.add_argument(
        "--reporting-year",
        help="Override reporting year Y (default: --year, REPORTING_YEAR env, or partition year)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    settings.refresh_from_env()

    if not settings.filter_prior_year_benefit_effective and not args.reporting_year:
        logger.warning(
            "FILTER_PRIOR_YEAR_BENEFIT_EFFECTIVE is false — running anyway because CLI invoked explicitly",
        )

    if args.reporting_year:
        settings.reporting_year = args.reporting_year
    if args.year:
        settings.reporting_year = settings.reporting_year or args.year

    settings.ensure_dirs()

    pairs = discover_issuer_year_pairs(issuer_filter=args.issuer, year_filter=args.year)
    if not pairs:
        logger.error("No issuer/year partitions found")
        sys.exit(1)

    reporting_override = None
    if args.reporting_year:
        reporting_override = str(args.reporting_year)
    elif args.year:
        reporting_override = resolve_reporting_year(cli_year=args.year)

    stats = run_prior_year_benefit_filter_batch(
        pairs, reporting_year_override=reporting_override,
    )

    logger.info("=" * 60)
    logger.info("PRIOR-YEAR BENEFIT FILTER COMPLETE")
    logger.info("Issuer/years : %d", stats.get("issuer_years", 0))
    logger.info("Output root  : %s", stats.get("output_root"))
    for r in stats.get("results", []):
        logger.info(
            "  %s/%s (Y=%s) raw %d→%d business %d→%d excluded %d",
            r["issuer"], r["year"], r["reporting_year"],
            r["raw_before"], r["raw_after"],
            r["business_before"], r["business_after"],
            r["excluded"],
        )
    if stats.get("diagnostics"):
        logger.info("Audit        : %s", stats["diagnostics"].get("audit_xlsx"))
        logger.info("Summary md   : %s", stats["diagnostics"].get("summary_md"))
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
