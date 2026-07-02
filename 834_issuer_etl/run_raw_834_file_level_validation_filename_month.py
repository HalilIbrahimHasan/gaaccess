#!/usr/bin/env python3
"""
File-level raw 834 XML validation with folder + filename month definitions.

Comparable to SQL grouped by GAA_834_File_Name and GAA_834_File_Date.

Reads source_data only. No business pipeline, no database, no Azure.

Example:
    python run_raw_834_file_level_validation_filename_month.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.config import settings
from raw_validation.raw_validation_common import (
    COVERAGE_YEAR,
    ISSUERS,
    OUTPUT_DIR,
    build_validation_info,
    parse_raw_rows,
)

OUTPUT_XLSX = OUTPUT_DIR / "raw_834_file_level_validation_filename_month_2025_3issuers.xlsx"

RAW_FILE_LEVEL_FILENAME_MONTH_COLUMNS = [
    "Issuer",
    "Folder_FileYear",
    "Folder_FileMonth",
    "Filename_FileYear",
    "Filename_FileMonth",
    "Insurance_Type",
    "enrolleeStatus",
    "Source_File",
    "Raw_XML_Row_Count",
    "Distinct_Policy_Count",
    "Distinct_Member_Count",
]

GROUP_COLUMNS = [
    "Issuer",
    "Folder_FileYear",
    "Folder_FileMonth",
    "Filename_FileYear",
    "Filename_FileMonth",
    "Insurance_Type",
    "enrolleeStatus",
    "Source_File",
]


def build_file_level_filename_month_report(work: pd.DataFrame) -> pd.DataFrame:
    if work.empty:
        return pd.DataFrame(columns=RAW_FILE_LEVEL_FILENAME_MONTH_COLUMNS)

    grouped = (
        work.groupby(GROUP_COLUMNS, dropna=False)
        .agg(
            Raw_XML_Row_Count=("Source_File", "size"),
            Distinct_Policy_Count=("policy_id", pd.Series.nunique),
            Distinct_Member_Count=("member_id", pd.Series.nunique),
        )
        .reset_index()
    )
    return grouped[RAW_FILE_LEVEL_FILENAME_MONTH_COLUMNS].sort_values(
        [
            "Issuer",
            "Folder_FileYear",
            "Folder_FileMonth",
            "Filename_FileYear",
            "Filename_FileMonth",
            "Insurance_Type",
            "enrolleeStatus",
            "Source_File",
        ],
        kind="stable",
    ).reset_index(drop=True)


def main() -> None:
    settings.refresh_from_env()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("\nRAW 834 FILE-LEVEL VALIDATION (FOLDER + FILENAME MONTH)")
    print(f"Coverage Year : {COVERAGE_YEAR}")
    print(f"Issuers       : {', '.join(ISSUERS)}")
    print(f"Source root   : {settings.source_data_path}")
    print(f"Output        : {OUTPUT_XLSX}")
    print("Scope: Parser834 raw output only (no business pipeline)")

    work, months_processed, failed_files, filename_warnings, total_source_files = parse_raw_rows()
    detail = build_file_level_filename_month_report(work)

    total_raw = len(work)
    distinct_policies = int(work["policy_id"].nunique(dropna=True)) if not work.empty else 0
    distinct_members = int(work["member_id"].nunique(dropna=True)) if not work.empty else 0

    info = build_validation_info(
        total_raw=total_raw,
        distinct_policies=distinct_policies,
        distinct_members=distinct_members,
        months_processed=months_processed,
        total_source_files=total_source_files,
        failed_files=failed_files,
        filename_parse_warnings=filename_warnings,
        output_path=OUTPUT_XLSX,
        report_type="RAW_FILE_LEVEL_FILENAME_MONTH (folder + filename month)",
    )

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        detail.to_excel(writer, sheet_name="RAW_FILE_LEVEL_FILENAME_MONTH", index=False)
        info.to_excel(writer, sheet_name="VALIDATION_INFO", index=False)

    print("\nRaw 834 file-level filename-month validation complete.")
    print(f"  File-level rows      : {len(detail)}")
    print(f"  Source files         : {total_source_files}")
    print(f"  Raw records          : {total_raw}")
    print(f"  Failed files         : {len(failed_files)}")
    print(f"  Filename parse warns : {len(filename_warnings)}")
    print(f"  Output XLSX          : {OUTPUT_XLSX}")

    if total_source_files == 0:
        print("\nNo source files found for the configured issuers/year.")
        sys.exit(1)


if __name__ == "__main__":
    main()
