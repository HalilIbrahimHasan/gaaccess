#!/usr/bin/env python3
"""
Best Life 2026 — residual enrollee file-level investigation exporter.

Usage:
    # Live Azure lookup (requires SERVER/DATABASE/USERNAME in environment):
    python run_best_life_residual_file_investigation.py

    # Use completed DB lookup export (CSV or Excel with enrollee_id + db_status):
    python run_best_life_residual_file_investigation.py \\
        --lookup-results path/to/best_life_db_lookup_results.csv

Output:
    last reports/best_life_residual_file_investigation.xlsx
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.best_life_residual_investigation import (
    build_investigation,
    load_lookup_results,
    load_swathi_candidates,
    lookup_ids_in_azure,
    normalize_id,
    write_investigation_workbook,
)
from inbound_automation.azure_common import connect_automation_engine
from utils.logger import get_logger

logger = get_logger(__name__)

DEFAULT_WORKBOOK = ROOT / "Best Life enrollments for 2026.xlsx"
DEFAULT_OUTPUT = ROOT / "last reports" / "best_life_residual_file_investigation.xlsx"
CANDIDATE_IDS_FILE = ROOT / "last reports" / "best_life_db_lookup_candidate_ids.txt"


def _load_full_swathi(workbook: Path) -> pd.DataFrame:
    df = pd.read_excel(workbook, sheet_name="Swathi", dtype=str).fillna("")
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()
    return df


def _load_selma(workbook: Path) -> pd.DataFrame:
    return pd.read_excel(workbook, sheet_name="Selma", dtype=str).fillna("")


def _resolve_lookup(
    candidates: pd.DataFrame,
    lookup_results_path: Path | None,
    skip_azure: bool,
) -> pd.DataFrame:
    unique_ids = sorted(candidates["enrollee_id_norm"].unique())
    CANDIDATE_IDS_FILE.parent.mkdir(parents=True, exist_ok=True)
    CANDIDATE_IDS_FILE.write_text("\n".join(unique_ids) + "\n", encoding="utf-8")
    logger.info("Wrote %s candidate enrollee IDs to %s", len(unique_ids), CANDIDATE_IDS_FILE)

    if lookup_results_path:
        logger.info("Loading lookup results from %s", lookup_results_path)
        lookup = load_lookup_results(lookup_results_path)
        missing = set(unique_ids) - set(lookup["enrollee_id"])
        if missing:
            logger.warning(
                "%s candidate IDs missing from lookup file; treating as NOT_FOUND",
                len(missing),
            )
            extra = pd.DataFrame(
                {"enrollee_id": sorted(missing), "db_status": "NOT_FOUND"}
            )
            lookup = pd.concat([lookup, extra], ignore_index=True)
        return lookup

    if skip_azure:
        raise SystemExit(
            "No --lookup-results provided and --skip-azure set. "
            f"Export DB lookup CSV or run sql/best_life_residual_db_lookup.sql "
            f"against candidate IDs in {CANDIDATE_IDS_FILE}"
        )

    try:
        engine = connect_automation_engine()
    except SystemExit as exc:
        raise SystemExit(
            "Azure lookup unavailable. Provide --lookup-results <csv|xlsx> "
            f"or configure SERVER/DATABASE/USERNAME. Candidate IDs: {CANDIDATE_IDS_FILE}"
        ) from exc

    logger.info("Running live Azure lookup for %s candidate IDs", len(unique_ids))
    return lookup_ids_in_azure(engine, unique_ids)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workbook", type=Path, default=DEFAULT_WORKBOOK)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--lookup-results",
        type=Path,
        default=None,
        help="CSV/Excel with enrollee_id and db_status (FOUND/NOT_FOUND)",
    )
    parser.add_argument(
        "--skip-azure",
        action="store_true",
        help="Do not attempt live Azure lookup when --lookup-results is absent",
    )
    args = parser.parse_args()

    if not args.workbook.exists():
        raise SystemExit(f"Workbook not found: {args.workbook}")

    candidates = load_swathi_candidates(args.workbook)
    full_swathi = _load_full_swathi(args.workbook)
    selma = _load_selma(args.workbook)

    lookup = _resolve_lookup(candidates, args.lookup_results, args.skip_azure)
    outputs = build_investigation(
        candidates,
        lookup,
        full_swathi=full_swathi,
        selma_df=selma,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    out = write_investigation_workbook(outputs, args.output)

    residual_count = int((lookup["db_status"] == "NOT_FOUND").sum())
    found_count = int((lookup["db_status"] == "FOUND").sum())
    print(f"Wrote {out}")
    print(f"Candidate pool (not in Selma Excel): {candidates['enrollee_id_norm'].nunique()}")
    print(f"DB FOUND: {found_count}")
    print(f"DB NOT_FOUND (residual): {residual_count}")
    print(f"Investigation rows: {len(outputs.residual_investigation)}")


if __name__ == "__main__":
    main()
