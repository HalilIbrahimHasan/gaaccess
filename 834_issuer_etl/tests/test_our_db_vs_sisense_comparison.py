from __future__ import annotations

import pandas as pd

import run_our_db_vs_sisense_comparison as report


def test_reference_matches_pdf_arithmetic() -> None:
    reference = report.load_sisense_reference()
    assert len(reference) == 37
    assert not reference.duplicated(["issuer", "insurance_type", "coverage_year"]).any()


def test_comparison_outer_join_and_statuses() -> None:
    ours = pd.DataFrame([
        {
            "HIOS Issuer ID": "10000",
            "Insurance Type": "Health",
            "Coverage Year": 2026,
            "Our Enrollments Total": 10,
            "Our Enrollees Total": 12,
            "Our Raw Rows": 20,
            "Rows Missing Enrollment Key": 0,
            "Rows Missing Enrollee Key": 0,
        },
        {
            "HIOS Issuer ID": "20000",
            "Insurance Type": "Dental",
            "Coverage Year": 2026,
            "Our Enrollments Total": 1,
            "Our Enrollees Total": 1,
            "Our Raw Rows": 1,
            "Rows Missing Enrollment Key": 0,
            "Rows Missing Enrollee Key": 0,
        },
    ])
    sisense = pd.DataFrame([
        {
            "issuer": "10000",
            "insurance_type": "HEALTH",
            "coverage_year": 2026,
            "sisense_enrollments_total": 10,
            "sisense_enrollees_total": 12,
            "sisense_enrollments_effectuated": 9,
            "sisense_enrollees_effectuated": 11,
            "sisense_enrollments_pending": 1,
            "sisense_enrollees_pending": 1,
        },
        {
            "issuer": "30000",
            "insurance_type": "Health",
            "coverage_year": 2026,
            "sisense_enrollments_total": 2,
            "sisense_enrollees_total": 2,
            "sisense_enrollments_effectuated": 2,
            "sisense_enrollees_effectuated": 2,
            "sisense_enrollments_pending": 0,
            "sisense_enrollees_pending": 0,
        },
    ])

    comparison = report.build_comparison(ours, sisense)
    statuses = dict(zip(comparison["HIOS Issuer ID"], comparison["Comparison Status"]))
    assert statuses == {
        "10000": "Exact Match",
        "20000": "Our Data Only",
        "30000": "Sisense Only",
    }
    exact = comparison[comparison["HIOS Issuer ID"] == "10000"].iloc[0]
    assert exact["Our Raw Rows"] == 20
    assert exact["sisense_enrollments_effectuated"] == 9
    assert exact["Comparison Scope"] == report.REPORT_SCOPE


def test_all_database_statements_are_select_only() -> None:
    for statement in (
        report.SOURCE_COUNT_SQL,
        report.OUR_SUMMARY_SQL,
        report.COVERAGE_DIAGNOSTICS_SQL,
    ):
        normalized = " ".join(str(statement).lower().split())
        assert normalized.startswith("select") or normalized.startswith("with")
        assert not any(
            token in f" {normalized} "
            for token in (" insert ", " update ", " delete ", " merge ", " alter ", " drop ")
        )
