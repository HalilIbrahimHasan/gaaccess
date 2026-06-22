#!/usr/bin/env python3
"""Classify remaining Chandra vs current dashboard enrollment differences (read-only)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.dashboard_difference_analysis import run_dashboard_difference_analysis
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    p = argparse.ArgumentParser(
        description="Explain current vs Chandra dashboard enrollment gaps (no logic changes)",
    )
    p.add_argument("--issuer", default="15105")
    p.add_argument("--year", default="2026")
    p.add_argument("--month", default="01")
    p.add_argument("--parse-source", action="store_true")
    args = p.parse_args()

    settings.refresh_from_env()
    settings.ensure_dirs()

    try:
        result = run_dashboard_difference_analysis(
            issuer=args.issuer,
            year=args.year,
            month=args.month,
            parse_source=args.parse_source,
        )
    except RuntimeError as exc:
        logger.error("%s", exc)
        sys.exit(1)

    print("\nDashboard difference analysis complete.")
    print(f"  Workbook: {result['xlsx']}")
    print(f"  Summary:  {result['summary_md']}")
    print(f"  Actual:   {result['actual_counts']}")
    print(f"  Expected: {result['expected_counts']}")
    print(f"  Sheets:   {result['sheets']}")


if __name__ == "__main__":
    main()
