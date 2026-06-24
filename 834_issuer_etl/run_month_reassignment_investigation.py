#!/usr/bin/env python3
"""Read-only month reassignment & cleaned-definition investigation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.month_reassignment_investigation import run_month_reassignment_investigation
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    p = argparse.ArgumentParser(
        description="Investigate cleaned export definition and business-month reassignment",
    )
    p.add_argument("--issuer", default="15105")
    p.add_argument("--year", default="2026")
    p.add_argument("--parse-source", action="store_true")
    p.add_argument(
        "--months",
        default="01,02,03,04,05",
        help="Comma-separated months for Chandra comparison (default Jan–May)",
    )
    args = p.parse_args()

    settings.refresh_from_env()
    settings.apply_xml_only_business_mode(True)
    settings.ensure_dirs()

    months = tuple(m.strip().zfill(2) for m in args.months.split(",") if m.strip())

    try:
        result = run_month_reassignment_investigation(
            issuer=args.issuer,
            year=args.year,
            parse_source=args.parse_source,
            months=months,
        )
    except RuntimeError as exc:
        logger.error("%s", exc)
        sys.exit(1)

    print("\nMonth reassignment investigation complete.")
    print(f"  Cleaned definition: {result['cleaned_definition_md']}")
    print(f"  Matrix:             {result['matrix_xlsx']}")
    print(f"  Reason summary:     {result['reason_xlsx']}")
    print(f"  Boundary analysis:  {result['boundary_xlsx']}")
    print(f"  Chandra comparison: {result['chandra_xlsx']}")
    print(f"  Conclusion:         {result['conclusion_md']}")
    print(f"  Moved records:      {result['moved_record_count']}")
    print(f"  Production gaps:    {result['production_gaps']}")


if __name__ == "__main__":
    main()
