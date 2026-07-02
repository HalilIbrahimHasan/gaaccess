#!/usr/bin/env python3
"""
Raw 834 XML validation — parser-only counts for comparison against dbo.[834_Inbound_test].

Reads source_data only. No business pipeline, no database, no Azure.

Example:
    python run_raw_834_validation.py
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.config import settings
from ingestion.file_discovery import discover_source_files
from ingestion.xml_reader import read_xml_bytes
from parsers.parser_834 import Parser834
from src.transform.enrollment_summary import (
    _insurance_type_display,
    _resolve_enrollee_status,
)
from utils.logger import get_logger

logger = get_logger(__name__)

COVERAGE_YEAR = "2025"
ISSUERS = ("13535", "15105", "43802")
OUTPUT_DIR = ROOT / "outputs" / "raw_validation"
OUTPUT_XLSX = OUTPUT_DIR / "raw_834_validation_2025_3issuers.xlsx"

RAW_VALIDATION_COLUMNS = [
    "Issuer",
    "Coverage_Year",
    "File_Year",
    "File_Month",
    "Insurance_Type",
    "enrolleeStatus",
    "Raw_XML_Record_Count",
    "Distinct_Policy_Count",
    "Distinct_Member_Count",
    "Source_File_Count",
    "First_Source_File",
    "Last_Source_File",
]


def _zmonth(month: str | int) -> str:
    return str(int(month)).zfill(2)


def _resolve_status(row: pd.Series) -> str:
    status = _resolve_enrollee_status(row)
    if status is None or (isinstance(status, float) and pd.isna(status)):
        return "UNMAPPED"
    return str(status).strip()


def _parse_all_sources() -> tuple[pd.DataFrame, int, int, list[str], list[str], int]:
    """Parse XML for fixed issuers/year."""
    parser = Parser834()
    source_root = settings.source_data_path
    all_rows: list[dict] = []
    failed_files: list[str] = []
    months_seen: set[str] = set()
    source_files: set[str] = set()

    for issuer in ISSUERS:
        sources = discover_source_files(
            source_root,
            issuer_filter=issuer,
            year_filter=COVERAGE_YEAR,
        )
        for src in sources:
            source_files.add(src.file_name)
            months_seen.add(_zmonth(src.month))
            try:
                xml_bytes = read_xml_bytes(src)
                records = parser.parse_file(
                    xml_bytes,
                    issuer=src.issuer,
                    year=src.year,
                    month=src.month,
                    file_name=src.file_name,
                    file_path=str(src.file_path),
                )
                for rec in records:
                    rec["source_file"] = src.file_name
                all_rows.extend(records)
            except Exception as exc:
                msg = f"{src.file_path}: {exc}"
                logger.error("Parse failed %s", msg)
                failed_files.append(msg)

    if not all_rows:
        empty = pd.DataFrame(columns=RAW_VALIDATION_COLUMNS)
        return empty, 0, 0, sorted(months_seen), failed_files, len(source_files)

    work = pd.DataFrame(all_rows)
    work["Issuer"] = work["issuer"].astype(str)
    work["Coverage_Year"] = int(COVERAGE_YEAR)
    work["File_Year"] = pd.to_numeric(work["year"], errors="coerce").astype("Int64")
    work["File_Month"] = pd.to_numeric(work["month"], errors="coerce").astype("Int64")
    work["Insurance_Type"] = work["insurance_type_code"].map(_insurance_type_display)
    work["enrolleeStatus"] = work.apply(_resolve_status, axis=1)

    distinct_policies = int(work["policy_id"].nunique(dropna=True))
    distinct_members = int(work["member_id"].nunique(dropna=True))

    grouped = (
        work.groupby(
            ["Issuer", "Coverage_Year", "File_Year", "File_Month", "Insurance_Type", "enrolleeStatus"],
            dropna=False,
        )
        .agg(
            Raw_XML_Record_Count=("source_file", "size"),
            Distinct_Policy_Count=("policy_id", pd.Series.nunique),
            Distinct_Member_Count=("member_id", pd.Series.nunique),
            Source_File_Count=("source_file", pd.Series.nunique),
            First_Source_File=("source_file", "min"),
            Last_Source_File=("source_file", "max"),
        )
        .reset_index()
    )
    grouped = grouped[RAW_VALIDATION_COLUMNS].sort_values(
        ["Issuer", "File_Year", "File_Month", "Insurance_Type", "enrolleeStatus"],
        kind="stable",
    ).reset_index(drop=True)
    return grouped, distinct_policies, distinct_members, sorted(months_seen), failed_files, len(source_files)


def _build_validation_info(
    detail: pd.DataFrame,
    *,
    distinct_policies: int,
    distinct_members: int,
    months_processed: list[str],
    total_source_files: int,
    failed_files: list[str],
    output_path: Path,
) -> pd.DataFrame:
    total_raw = int(detail["Raw_XML_Record_Count"].sum()) if not detail.empty else 0
    rows = [
        ("Coverage Year", COVERAGE_YEAR),
        ("Issuers processed", ", ".join(ISSUERS)),
        ("Months processed", ", ".join(months_processed) if months_processed else "(none)"),
        ("Total source files", total_source_files),
        ("Total raw records", total_raw),
        ("Distinct policies", distinct_policies),
        ("Distinct members", distinct_members),
        ("Failed files", "; ".join(failed_files) if failed_files else "(none)"),
        ("Execution timestamp", datetime.now(timezone.utc).isoformat(timespec="seconds")),
        ("Output path", str(output_path)),
    ]
    return pd.DataFrame(rows, columns=["Field", "Value"])


def main() -> None:
    settings.refresh_from_env()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("\nRAW 834 VALIDATION")
    print(f"Coverage Year : {COVERAGE_YEAR}")
    print(f"Issuers       : {', '.join(ISSUERS)}")
    print(f"Source root   : {settings.source_data_path}")
    print(f"Output        : {OUTPUT_XLSX}")
    print("Scope: Parser834 raw output only (no business pipeline)")

    detail, distinct_policies, distinct_members, months_processed, failed_files, total_source_files = (
        _parse_all_sources()
    )
    info = _build_validation_info(
        detail,
        distinct_policies=distinct_policies,
        distinct_members=distinct_members,
        months_processed=months_processed,
        total_source_files=total_source_files,
        failed_files=failed_files,
        output_path=OUTPUT_XLSX,
    )

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        detail.to_excel(writer, sheet_name="RAW_VALIDATION", index=False)
        info.to_excel(writer, sheet_name="VALIDATION_INFO", index=False)

    print("\nRaw 834 validation complete.")
    print(f"  Summary rows : {len(detail)}")
    print(f"  Source files : {total_source_files}")
    print(f"  Raw records  : {info.loc[info['Field'] == 'Total raw records', 'Value'].iloc[0]}")
    print(f"  Failed files : {len(failed_files)}")
    print(f"  Output XLSX  : {OUTPUT_XLSX}")

    if total_source_files == 0:
        print("\nNo source files found for the configured issuers/year.")
        sys.exit(1)


if __name__ == "__main__":
    main()
