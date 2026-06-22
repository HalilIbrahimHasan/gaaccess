#!/usr/bin/env python3
"""Read-only audit: raw XML row → canonical row cardinality (debug only)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.canonical_row_expansion_audit import write_canonical_expansion_audit
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    p = argparse.ArgumentParser(
        description="Audit raw XML → canonical row expansion for one issuer/month",
    )
    p.add_argument("--issuer", default="15105")
    p.add_argument("--year", default="2026")
    p.add_argument("--month", default="01")
    p.add_argument("--parse-source", action="store_true")
    args = p.parse_args()

    settings.refresh_from_env()
    settings.apply_xml_only_business_mode(True)
    settings.ensure_dirs()

    try:
        xlsx_path, md_path = write_canonical_expansion_audit(
            issuer=args.issuer,
            year=args.year,
            month=args.month,
            parse_source=args.parse_source,
        )
    except RuntimeError as exc:
        logger.error("%s", exc)
        sys.exit(1)

    print(f"Wrote {xlsx_path}")
    print(f"Wrote {md_path}")


if __name__ == "__main__":
    main()
