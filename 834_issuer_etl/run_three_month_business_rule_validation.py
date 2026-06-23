#!/usr/bin/env python3
"""Read-only three-month business rule investigation vs Chandra dashboard gaps."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.three_month_business_rule_validation import (
    run_three_month_business_rule_validation,
)
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    p = argparse.ArgumentParser(
        description="Three-month rolling window business rule validation (read-only)",
    )
    p.add_argument("--issuer", default="15105")
    p.add_argument("--year", default="2026")
    p.add_argument("--month", default="01")
    p.add_argument("--parse-source", action="store_true")
    p.add_argument("--expected-confirm", type=int, default=None)
    p.add_argument("--expected-cancel", type=int, default=None)
    p.add_argument("--expected-term", type=int, default=None)
    args = p.parse_args()

    settings.refresh_from_env()
    settings.apply_xml_only_business_mode(True)
    settings.ensure_dirs()

    try:
        result = run_three_month_business_rule_validation(
            issuer=args.issuer,
            year=args.year,
            month=args.month,
            parse_source=args.parse_source,
            cli_confirm=args.expected_confirm,
            cli_cancel=args.expected_cancel,
            cli_term=args.expected_term,
        )
    except RuntimeError as exc:
        logger.error("%s", exc)
        sys.exit(1)

    print("\nThree-month business rule validation complete.")
    print(f"  Workbook:      {result['xlsx']}")
    print(f"  Summary:       {result['summary_md']}")
    print(f"  Monthly XLSX:  {result['monthly_xlsx']}")
    print(f"  Monthly HTML:  {result['monthly_html']}")
    print(f"  Status:        {result['status_summaries']}")
    print(f"  Verdict:       {result['hypothesis_verdict']}")
    if result.get("best_rule"):
        print(f"  Best rule:     {result['best_rule'].get('Rule_Name')} "
              f"({result['best_rule'].get('Pct_Gap_Explained')}%)")


if __name__ == "__main__":
    main()
