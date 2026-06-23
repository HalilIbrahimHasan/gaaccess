#!/usr/bin/env python3
"""Read-only reason-field business investigation vs Chandra dashboard gaps."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.reason_investigation import run_reason_investigation
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    p = argparse.ArgumentParser(
        description="Reason-field business investigation (read-only simulation)",
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
        result = run_reason_investigation(
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

    print("\nReason investigation complete.")
    print(f"  Field discovery: {result['discovery_xlsx']}")
    print(f"  Value frequency: {result['frequency_xlsx']}")
    print(f"  Rule impact:     {result['rule_impact_xlsx']}")
    print(f"  Combinations:    {result['combination_xlsx']}")
    print(f"  Summary:         {result['summary_md']}")
    print(f"  Recommended:     {result['recommended_fields']}")
    print(f"  Current:         {result['current_counts']}")
    print(f"  Expected:        {result['expected_counts']}")
    if result.get("best_rule"):
        br = result["best_rule"]
        print(f"  Best rule:       {br.get('column_name')}={br.get('reason_value')} "
              f"(score +{br.get('total_gap_score_improvement')})")


if __name__ == "__main__":
    main()
