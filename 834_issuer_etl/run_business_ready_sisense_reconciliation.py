#!/usr/bin/env python3
"""Run the read-only Business Ready-to-Sisense staged reconciliation."""

from __future__ import annotations

import argparse
from pathlib import Path

from azure_reconciliation.azure_client import connect_azure
from azure_reconciliation.business_ready_sisense_reconciliation import (
    DEFAULT_EXPECTED_COUNTS,
    run_reconciliation,
)
from config.config import settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "SELECT dbo.inbound_automation, apply existing Business Ready rules, "
            "and write a local Sisense reconciliation workbook."
        )
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        choices=sorted(DEFAULT_EXPECTED_COUNTS),
        default=sorted(DEFAULT_EXPECTED_COUNTS),
        help="Folder years to reconcile (default: 2025 2026).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=(
            settings.outputs_path
            / "sisense_reconciliation"
            / "business_ready_sisense_reconciliation.xlsx"
        ),
        help="Local output workbook path.",
    )
    parser.add_argument(
        "--allow-count-mismatch",
        action="store_true",
        help=(
            "Continue for investigation when validated Azure counts differ from "
            "the approved counts. Mismatches remain ERROR checks in the workbook."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    engine, _ = connect_azure(
        runner_name="run_business_ready_sisense_reconciliation",
        strict=True,
    )
    if engine is None:
        raise RuntimeError("Azure connection was not created")
    try:
        result = run_reconciliation(
            engine,
            years=args.years,
            output_path=args.output.resolve(),
            strict_counts=not args.allow_count_mismatch,
        )
    finally:
        engine.dispose()

    print(f"Wrote: {result.output_path}")
    print(result.run_summary.to_string(index=False))
    failed = result.data_quality_checks[
        ~result.data_quality_checks["passed"].astype(bool)
    ]
    if not failed.empty:
        print("\nData-quality failures:")
        print(failed.to_string(index=False))
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
