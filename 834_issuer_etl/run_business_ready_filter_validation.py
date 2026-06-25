#!/usr/bin/env python3
"""Read-only Business Ready prior-year filter validation investigation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.business_ready_filter_validation import run_business_ready_filter_validation
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    p = argparse.ArgumentParser(
        description="Validate Business Ready prior-year benefit filter with evidence",
    )
    p.add_argument("--issuer", default="15105")
    p.add_argument("--year", default="2026")
    p.add_argument("--reporting-year", default=None, help="Defaults to partition year or REPORTING_YEAR")
    p.add_argument("--parse-source", action="store_true")
    args = p.parse_args()

    settings.refresh_from_env()
    settings.apply_xml_only_business_mode(True)
    settings.ensure_dirs()

    try:
        result = run_business_ready_filter_validation(
            issuer=args.issuer,
            year=args.year,
            reporting_year=args.reporting_year,
            parse_source=args.parse_source,
        )
    except RuntimeError as exc:
        logger.error("%s", exc)
        sys.exit(1)

    print("\nBusiness Ready filter validation complete.")
    print(f"  Column inventory:     {result['column_inventory']}")
    print(f"  Prior-year candidates:{result['prior_year_candidates']}")
    print(f"  Validation report:    {result['validation_md']}")
    print(f"  Engineering conclusion:{result['engineering_conclusion']}")
    print(f"  Candidates (benefit): {result['prior_year_candidates_benefit']}")
    print(f"  Candidates (selected): {result['prior_year_candidates_selected']}")


if __name__ == "__main__":
    main()
