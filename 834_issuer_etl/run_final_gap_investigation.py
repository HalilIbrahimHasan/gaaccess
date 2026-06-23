#!/usr/bin/env python3
"""Final read-only investigation — explain remaining Chandra dashboard gaps."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.final_gap_investigation import run_final_gap_investigation
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    p = argparse.ArgumentParser(
        description="Final gap investigation — explain every remaining enrollment difference",
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
        result = run_final_gap_investigation(
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

    print("\nFinal gap investigation complete.")
    print(f"  History:     {result['history_xlsx']}")
    print(f"  Selection:   {result['selection_xlsx']}")
    print(f"  Timelines:   {result['gap_timelines_xlsx']}")
    print(f"  Categories:  {result['categories_xlsx']}")
    print(f"  Final:       {result['final_xlsx']}")
    print(f"  Conclusion:  {result['conclusion_md']}")
    print(f"  Gap count:   {result['gap_enrollment_count']}")
    print(f"  Current:     {result['actual_counts']}")
    print(f"  Expected:    {result['expected_counts']}")
    if result.get("categories"):
        print(f"  Categories:  {result['categories']}")


if __name__ == "__main__":
    main()
