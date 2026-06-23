#!/usr/bin/env python3
"""Read-only maintenance code business investigation vs Chandra dashboard gaps."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.maintenance_investigation import run_maintenance_investigation
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    p = argparse.ArgumentParser(
        description="Maintenance code business investigation (read-only simulation)",
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
        result = run_maintenance_investigation(
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

    print("\nMaintenance investigation complete.")
    print(f"  Frequency:    {result['frequency_xlsx']}")
    print(f"  Rule impact:  {result['rule_impact_xlsx']}")
    print(f"  Combinations: {result['combination_xlsx']}")
    print(f"  Reasons:      {result['reason_xlsx']}")
    print(f"  Summary:      {result['summary_md']}")
    print(f"  Current:      {result['current_counts']}")
    print(f"  Expected:     {result['expected_counts']}")
    if result.get("top_code"):
        tc = result["top_code"]
        print(f"  Top code:     {tc.get('Maintenance_Code')} "
              f"(gap reduced {tc.get('Gap_Reduced')})")
    if result.get("best_combination"):
        bc = result["best_combination"]
        print(f"  Best combo:   {bc.get('Combination')} "
              f"(gap reduced {bc.get('Gap_Reduced')})")


if __name__ == "__main__":
    main()
