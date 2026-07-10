#!/usr/bin/env python3
"""
Full structural XML completeness audit for 2025 and 2026 source_data.

Scans every discovered XML file and writes Excel reports under last reports/.

Usage:
    python run_xml_structural_audit.py
    python run_xml_structural_audit.py --source-root /path/to/source_data
    python run_xml_structural_audit.py --output-dir /path/to/reports
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from raw_validation.xml_structural_audit import AUDIT_YEARS, run_structural_audit
from utils.logger import get_logger

logger = get_logger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Structural Parser834 compatibility audit for source_data XML",
    )
    parser.add_argument(
        "--source-root",
        type=Path,
        default=None,
        help="Override source_data root (default: settings.source_data_path)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Report output directory (default: 834_issuer_etl/last reports)",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        default=list(AUDIT_YEARS),
        help="Folder years to audit (default: 2025 2026)",
    )
    args = parser.parse_args(argv)

    try:
        result = run_structural_audit(
            source_root=args.source_root,
            output_dir=args.output_dir,
            years=tuple(args.years),
        )
    except Exception:
        logger.exception("Structural audit failed")
        return 1

    ok = result.successful_rows
    total_direct = sum(int(r.get("direct_enrollee_count") or 0) for r in ok)
    total_parser = sum(int(r.get("parser_row_count") or 0) for r in ok)

    print(f"Source root audited : {result.source_root}")
    print(f"Years               : {', '.join(result.years)}")
    print(f"Files discovered    : {len(result.file_rows)}")
    print(f"Files audited OK    : {len(ok)}")
    print(f"Files with errors   : {len(result.failed_rows)}")
    print(f"Direct enrollee cnt : {total_direct:,}")
    print(f"Parser row count    : {total_parser:,}")
    print(f"Parser == direct    : {total_parser == total_direct}")
    print(f"Reports written to  : {result.output_dir}")
    for name in (
        "structural_audit_summary.xlsx",
        "files_with_namespace.xlsx",
        "files_with_nested_enrollees.xlsx",
        "empty_enrollments.xlsx",
        "repeated_enrollee_blocks.xlsx",
        "parser_row_count_mismatches.xlsx",
    ):
        print(f"  - {name}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
