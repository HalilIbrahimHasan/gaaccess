#!/usr/bin/env python3
"""Export an initial annual distinct comparison of Azure raw data and Sisense."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.safe_export import safe_write_excel
from inbound_automation.azure_common import connect_automation_engine

EXPECTED_SOURCE_COUNTS = {2025: 2_141_037, 2026: 1_029_577}
REPORT_SCOPE = (
    "Initial annual distinct comparison — not final Business Ready equivalence"
)
REFERENCE_CSV = (
    ROOT / "reference" / "sisense_effectuated_enrollments_2026-07-14.csv"
)
DEFAULT_OUTPUT = ROOT / "last reports" / "our_db_vs_sisense_comparison.xlsx"

SOURCE_COUNT_SQL = text("""
SELECT
    folder_year,
    COUNT_BIG(*) AS source_row_count
FROM dbo.inbound_automation
WHERE folder_year IN (2025, 2026)
GROUP BY folder_year
ORDER BY folder_year
""")

OUR_SUMMARY_SQL = text("""
WITH source_rows AS (
    SELECT
        NULLIF(LTRIM(RTRIM(issuer)), '') AS issuer,
        CASE
            WHEN UPPER(NULLIF(LTRIM(RTRIM(insurance_type)), '')) IN ('HEALTH', 'HLT')
                THEN 'Health'
            WHEN UPPER(NULLIF(LTRIM(RTRIM(insurance_type)), '')) IN ('DENTAL', 'DEN')
                THEN 'Dental'
            ELSE NULLIF(LTRIM(RTRIM(insurance_type)), '')
        END AS insurance_type,
        folder_year,
        coverage_year,
        COALESCE(
            NULLIF(LTRIM(RTRIM(policy_id)), ''),
            NULLIF(LTRIM(RTRIM(health_coverage_policy_no)), '')
        ) AS enrollment_key,
        COALESCE(
            NULLIF(LTRIM(RTRIM(member_id)), ''),
            NULLIF(LTRIM(RTRIM(issuer_indiv_identifier)), ''),
            NULLIF(LTRIM(RTRIM(exchg_assigned_enrollee_id)), '')
        ) AS enrollee_key
    FROM dbo.inbound_automation
    WHERE folder_year IN (2025, 2026)
),
comparison_population AS (
    SELECT *
    FROM source_rows
    WHERE coverage_year IN (2025, 2026)
      AND issuer IS NOT NULL
      AND insurance_type IN ('Health', 'Dental')
)
SELECT
    issuer AS [HIOS Issuer ID],
    insurance_type AS [Insurance Type],
    coverage_year AS [Coverage Year],
    COUNT(DISTINCT enrollment_key) AS [Our Enrollments Total],
    COUNT(DISTINCT enrollee_key) AS [Our Enrollees Total],
    COUNT_BIG(*) AS [Our Raw Rows],
    SUM(CASE WHEN enrollment_key IS NULL THEN 1 ELSE 0 END)
        AS [Rows Missing Enrollment Key],
    SUM(CASE WHEN enrollee_key IS NULL THEN 1 ELSE 0 END)
        AS [Rows Missing Enrollee Key]
FROM comparison_population
GROUP BY issuer, insurance_type, coverage_year
ORDER BY issuer, insurance_type, coverage_year
""")

COVERAGE_DIAGNOSTICS_SQL = text("""
WITH source_rows AS (
    SELECT
        folder_year,
        coverage_year,
        NULLIF(LTRIM(RTRIM(issuer)), '') AS issuer,
        CASE
            WHEN UPPER(NULLIF(LTRIM(RTRIM(insurance_type)), '')) IN ('HEALTH', 'HLT')
                THEN 'Health'
            WHEN UPPER(NULLIF(LTRIM(RTRIM(insurance_type)), '')) IN ('DENTAL', 'DEN')
                THEN 'Dental'
            ELSE NULLIF(LTRIM(RTRIM(insurance_type)), '')
        END AS insurance_type,
        COALESCE(
            NULLIF(LTRIM(RTRIM(policy_id)), ''),
            NULLIF(LTRIM(RTRIM(health_coverage_policy_no)), '')
        ) AS enrollment_key,
        COALESCE(
            NULLIF(LTRIM(RTRIM(member_id)), ''),
            NULLIF(LTRIM(RTRIM(issuer_indiv_identifier)), ''),
            NULLIF(LTRIM(RTRIM(exchg_assigned_enrollee_id)), '')
        ) AS enrollee_key
    FROM dbo.inbound_automation
    WHERE folder_year IN (2025, 2026)
)
SELECT
    folder_year AS [Folder Year],
    coverage_year AS [Coverage Year],
    CASE
        WHEN coverage_year IS NULL THEN 'Missing Coverage Year'
        WHEN coverage_year NOT IN (2025, 2026) THEN 'Outside Comparison Years'
        WHEN coverage_year = folder_year THEN 'Folder/Coverage Match'
        ELSE 'Folder/Coverage Mismatch'
    END AS [Folder vs Coverage Diagnostic],
    COUNT_BIG(*) AS [Raw Rows],
    SUM(CASE WHEN issuer IS NULL THEN 1 ELSE 0 END)
        AS [Rows Missing Issuer],
    SUM(CASE WHEN insurance_type NOT IN ('Health', 'Dental')
                  OR insurance_type IS NULL THEN 1 ELSE 0 END)
        AS [Rows Outside Health Dental],
    SUM(CASE WHEN enrollment_key IS NULL THEN 1 ELSE 0 END)
        AS [Rows Missing Enrollment Key],
    SUM(CASE WHEN enrollee_key IS NULL THEN 1 ELSE 0 END)
        AS [Rows Missing Enrollee Key]
FROM source_rows
GROUP BY
    folder_year,
    coverage_year,
    CASE
        WHEN coverage_year IS NULL THEN 'Missing Coverage Year'
        WHEN coverage_year NOT IN (2025, 2026) THEN 'Outside Comparison Years'
        WHEN coverage_year = folder_year THEN 'Folder/Coverage Match'
        ELSE 'Folder/Coverage Mismatch'
    END
ORDER BY folder_year, coverage_year
""")


def validate_source_counts(engine) -> pd.DataFrame:
    with engine.connect() as conn:
        counts = pd.read_sql(SOURCE_COUNT_SQL, conn)
    actual = {
        int(row.folder_year): int(row.source_row_count)
        for row in counts.itertuples(index=False)
    }
    failures = [
        f"{year}: expected {expected:,}, found {actual.get(year, 0):,}"
        for year, expected in EXPECTED_SOURCE_COUNTS.items()
        if actual.get(year, 0) != expected
    ]
    combined_expected = sum(EXPECTED_SOURCE_COUNTS.values())
    combined_actual = sum(actual.get(year, 0) for year in EXPECTED_SOURCE_COUNTS)
    if combined_actual != combined_expected:
        failures.append(
            f"combined: expected {combined_expected:,}, found {combined_actual:,}"
        )
    if failures:
        raise RuntimeError(
            "Azure source-count validation failed; comparison was not run. "
            + "; ".join(failures)
        )
    print(
        "Validated source rows: "
        f"2025={actual[2025]:,}, 2026={actual[2026]:,}, "
        f"combined={combined_actual:,}"
    )
    return counts


def read_sql_results(engine) -> tuple[pd.DataFrame, pd.DataFrame]:
    with engine.connect() as conn:
        summary = pd.read_sql(OUR_SUMMARY_SQL, conn)
        diagnostics = pd.read_sql(COVERAGE_DIAGNOSTICS_SQL, conn)
    return summary, diagnostics


def load_sisense_reference(path: Path = REFERENCE_CSV) -> pd.DataFrame:
    reference = pd.read_csv(path, dtype={"issuer": str})
    key_columns = ["issuer", "insurance_type", "coverage_year"]
    if reference.duplicated(key_columns).any():
        raise RuntimeError("Sisense reference contains duplicate issuer/type/year rows")
    enrollment_total = (
        reference["sisense_enrollments_effectuated"]
        + reference["sisense_enrollments_pending"]
    )
    enrollee_total = (
        reference["sisense_enrollees_effectuated"]
        + reference["sisense_enrollees_pending"]
    )
    if not (
        enrollment_total.eq(reference["sisense_enrollments_total"]).all()
        and enrollee_total.eq(reference["sisense_enrollees_total"]).all()
    ):
        raise RuntimeError("Sisense reference metric arithmetic failed")
    return reference


def build_comparison(ours: pd.DataFrame, sisense: pd.DataFrame) -> pd.DataFrame:
    our_keys = ours.copy()
    our_keys["_issuer"] = our_keys["HIOS Issuer ID"].astype(str).str.strip()
    our_keys["_insurance_type"] = (
        our_keys["Insurance Type"].astype(str).str.strip().str.upper()
    )
    our_keys["_coverage_year"] = pd.to_numeric(
        our_keys["Coverage Year"], errors="coerce"
    ).astype("Int64")

    sisense_keys = sisense.copy()
    sisense_keys["_issuer"] = sisense_keys["issuer"].astype(str).str.strip()
    sisense_keys["_insurance_type"] = (
        sisense_keys["insurance_type"].astype(str).str.strip().str.upper()
    )
    sisense_keys["_coverage_year"] = pd.to_numeric(
        sisense_keys["coverage_year"], errors="coerce"
    ).astype("Int64")

    merged = our_keys.merge(
        sisense_keys,
        on=["_issuer", "_insurance_type", "_coverage_year"],
        how="outer",
        indicator=True,
    )
    merged["HIOS Issuer ID"] = merged["_issuer"]
    merged["Insurance Type"] = merged["Insurance Type"].fillna(
        merged["insurance_type"]
    )
    merged["Coverage Year"] = merged["_coverage_year"]

    for entity in ("Enrollment", "Enrollee"):
        ours_column = f"Our {entity}s Total"
        sisense_column = f"sisense_{entity.lower()}s_total"
        difference_column = f"{entity} Difference"
        merged[difference_column] = merged[ours_column] - merged[sisense_column]
        merged[f"{entity} Absolute Difference"] = merged[
            difference_column
        ].abs()
        merged[f"{entity} Difference %"] = (
            100
            * merged[difference_column]
            / merged[sisense_column].replace(0, pd.NA)
        ).round(2)

    merged["Comparison Status"] = "Difference"
    merged.loc[merged["_merge"] == "left_only", "Comparison Status"] = (
        "Our Data Only"
    )
    merged.loc[merged["_merge"] == "right_only", "Comparison Status"] = (
        "Sisense Only"
    )
    exact = (
        merged["_merge"].eq("both")
        & merged["Enrollment Difference"].eq(0)
        & merged["Enrollee Difference"].eq(0)
    )
    merged.loc[exact, "Comparison Status"] = "Exact Match"
    merged["Comparison Scope"] = REPORT_SCOPE

    columns = [
        "Comparison Scope",
        "HIOS Issuer ID",
        "Insurance Type",
        "Coverage Year",
        "Our Enrollments Total",
        "Our Enrollees Total",
        "Our Raw Rows",
        "Rows Missing Enrollment Key",
        "Rows Missing Enrollee Key",
        "sisense_enrollments_total",
        "sisense_enrollees_total",
        "sisense_enrollments_effectuated",
        "sisense_enrollees_effectuated",
        "sisense_enrollments_pending",
        "sisense_enrollees_pending",
        "Enrollment Difference",
        "Enrollment Absolute Difference",
        "Enrollment Difference %",
        "Enrollee Difference",
        "Enrollee Absolute Difference",
        "Enrollee Difference %",
        "Comparison Status",
    ]
    return merged[columns].sort_values(
        ["HIOS Issuer ID", "Insurance Type", "Coverage Year"],
        kind="stable",
    )


def reference_sheet(reference: pd.DataFrame) -> pd.DataFrame:
    return reference.rename(columns={
        "issuer": "HIOS Issuer ID",
        "insurance_type": "Insurance Type",
        "coverage_year": "Coverage Year",
        "sisense_enrollments_total": "Sisense Enrollments Total",
        "sisense_enrollees_total": "Sisense Enrollees Total",
        "sisense_enrollments_effectuated": "Sisense Enrollments Effectuated",
        "sisense_enrollees_effectuated": "Sisense Enrollees Effectuated",
        "sisense_enrollments_pending": "Sisense Enrollments Pending",
        "sisense_enrollees_pending": "Sisense Enrollees Pending",
    })


def run(output_path: Path) -> Path:
    engine = connect_automation_engine(fast_executemany=False)
    try:
        validate_source_counts(engine)
        ours, diagnostics = read_sql_results(engine)
    finally:
        engine.dispose()

    ours.insert(0, "Comparison Scope", REPORT_SCOPE)
    diagnostics.insert(0, "Comparison Scope", REPORT_SCOPE)
    sisense = load_sisense_reference()
    comparison = build_comparison(ours, sisense)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    written = safe_write_excel(
        output_path,
        {
            "Our_DB_Summary": ours,
            "Sisense_Reference": reference_sheet(sisense),
            "Comparison": comparison,
            "Coverage_Year_Diagnostics": diagnostics,
        },
        drop_duplicate_value_columns=False,
    )
    if not written:
        raise RuntimeError(f"Excel export failed: {output_path}")
    return output_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=REPORT_SCOPE,
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)
    print(REPORT_SCOPE)
    output = run(args.output.resolve())
    print(f"Wrote {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
