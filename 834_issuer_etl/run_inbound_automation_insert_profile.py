#!/usr/bin/env python3
"""
Profile Azure insert performance for inbound automation.

Uses rolled-back transactions — no rows are persisted.

Examples:
    INBOUND_AUTOMATION_ENABLED=true python run_inbound_automation_insert_profile.py \\
        --issuer 15105 --year 2025 --month 10

    INBOUND_AUTOMATION_ENABLED=true python run_inbound_automation_insert_profile.py \\
        --issuer 15105 --year 2025 --month 10 --batch-sizes 1000,5000,10000 --max-files 20
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from inbound_automation.constants import OUTPUT_ROOT
from inbound_automation.insert_profiler import INSERT_METHOD, run_insert_profile


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Profile inbound_automation Azure insert performance (rollback only).",
    )
    parser.add_argument("--issuer", type=str, help="Issuer HIOS id filter.")
    parser.add_argument("--year", type=str, required=True, help="Year filter (required).")
    parser.add_argument("--month", type=str, help="Month filter (1-12).")
    parser.add_argument(
        "--batch-sizes",
        type=str,
        default="1000,5000,10000",
        help="Comma-separated batch sizes to benchmark (default: 1000,5000,10000).",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=10,
        help="Max source files to parse for sample rows (default: 10).",
    )
    args = parser.parse_args(argv)

    batch_sizes = [int(x.strip()) for x in args.batch_sizes.split(",") if x.strip()]
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = OUTPUT_ROOT / f"insert_profile_{ts}"
    output_dir.mkdir(parents=True, exist_ok=True)

    print("\nINBOUND AUTOMATION — INSERT PROFILE (rollback only)")
    print(f"  issuer      : {args.issuer or 'ALL'}")
    print(f"  year        : {args.year}")
    print(f"  month       : {args.month or 'ALL'}")
    print(f"  batch sizes : {batch_sizes}")
    print(f"  max files   : {args.max_files}")
    print(f"  output      : {output_dir}")
    print(f"\nInsert method: {INSERT_METHOD}")
    print("NOT row-by-row. NOT SQLAlchemy ORM bulk. Uses executemany via list[dict].")

    result = run_insert_profile(
        issuer=args.issuer,
        year=args.year,
        month=args.month,
        batch_sizes=batch_sizes,
        max_files=args.max_files,
        output_dir=output_dir,
    )

    print("\nProfile complete (no rows persisted).")
    print(f"  sample rows  : {result['sample_rows']}")
    print(f"  sample files : {result['sample_files']}")
    print(f"  report       : {result['output_path']}")

    # Print recommendation from best ON result
    on_results = [r for r in result["summary"] if r.get("fast_executemany") and r.get("profile_type") != "per_file"]
    if on_results:
        best = max(on_results, key=lambda r: r.get("avg_rows_per_sec", 0))
        print("\nRecommended starting point (fast_executemany=ON):")
        print(f"  batch_size={best['batch_size']} → {best['avg_rows_per_sec']} rows/sec")
        print("  Set: INBOUND_AUTOMATION_BATCH_SIZE=<value>")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
