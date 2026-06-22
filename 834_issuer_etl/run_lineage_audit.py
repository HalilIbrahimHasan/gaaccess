#!/usr/bin/env python3
"""Write lineage audit CSV/MD for one issuer/month (debug only)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.lineage_audit import write_lineage_audit
from azure_reconciliation.partition_discovery import discover_partitions
from azure_reconciliation.xml_loader import load_xml_rows
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    p = argparse.ArgumentParser(description="Lineage row-count audit for one issuer/month")
    p.add_argument("--issuer", default="15105")
    p.add_argument("--year", default="2026")
    p.add_argument("--month", default="01")
    p.add_argument("--parse-source", action="store_true")
    args = p.parse_args()

    settings.refresh_from_env()
    settings.apply_xml_only_business_mode(True)
    settings.ensure_dirs()

    partitions = discover_partitions(settings.source_data_path, issuer_filter=args.issuer)
    xml_raw = load_xml_rows(
        prefer_staging=not args.parse_source,
        issuer_filter=args.issuer,
    )
    if xml_raw.empty:
        logger.error("No XML rows for issuer %s", args.issuer)
        sys.exit(1)

    csv_path, md_path = write_lineage_audit(
        xml_raw,
        partitions,
        issuer=args.issuer,
        year=args.year,
        month=args.month,
    )
    print(f"Wrote {csv_path}")
    print(f"Wrote {md_path}")


if __name__ == "__main__":
    main()
