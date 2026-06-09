"""
Export partition and rollup assets: excel, cleaned_xml, sqlite, dashboards.

Reads from staging SQLite and writes to assets/{issuer}/{year}/{month}/.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

_SRC = settings.project_root / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from dashboard.plotly_dashboard import PlotlyDashboard  # noqa: E402
from load.excel_exporter import ExcelExporter  # noqa: E402
from load.sqlite_loader import SqliteLoader  # noqa: E402
from load.xml_exporter import XmlExporter  # noqa: E402
from transform.kpi_builder import KpiBuilder  # noqa: E402
from validate.data_quality_validator import DataQualityValidator  # noqa: E402
from validate.schema_validator import SchemaValidator  # noqa: E402


STG_TO_LEGACY = {
    "issuer": "issuer_id",
    "policy_id": "exchg_assigned_policy_id",
    "member_id": "exchg_indiv_identifier",
    "subscriber_id": "exchg_subscriber_identifier",
    "relationship": "relationship_code",
    "total_premium_amount": "total_premium_amt",
    "individual_responsibility_amount": "total_indiv_responsibility_amt",
    "aptc_amount": "aptc_amt",
    "insurance_type_code": "insurance_type_code",
    "benefit_effective_date": "benefit_effective_begin_date",
    "member_maint_effective_date": "member_maint_effective_date",
    "action_code": "event_type_code",
    "additional_maint_reason_code": "additional_maint_reason_code",
}


def _partition_dirs(issuer: str, year: str, month: str) -> dict[str, Path]:
    base = settings.assets_path / issuer / year / month
    return {
        "base": base,
        "excel": base / "excel",
        "cleaned_xml": base / "cleaned_xml",
        "sqlite": base / "sqlite",
        "dashboards": base / "dashboards",
        "validation_reports": base / "validation_reports",
    }


def _rollup_dirs(issuer: str) -> dict[str, Path]:
    base = settings.assets_path / issuer / "rollups"
    return {
        "base": base,
        "excel": base / "excel",
        "sqlite": base / "sqlite",
        "dashboards": base / "dashboards",
        "validation_reports": base / "validation_reports",
    }


def _stg_to_legacy_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = out.rename(columns=STG_TO_LEGACY)
    if "source_file" not in out.columns and "file_name" in out.columns:
        out["source_file"] = out["file_name"]
    if "year" in out.columns:
        out["source_year"] = out["year"].astype(str)
    if "month" in out.columns:
        out["source_month"] = out["month"].astype(str)
    if "source_year" in out.columns and "source_month" in out.columns:
        out["source_period"] = out["source_year"] + "-" + out["source_month"]
    for col in ("load_timestamp", "file_date", "event_reason_code", "rating_area",
                "household_or_employee_case_id", "source_exchg_id"):
        if col not in out.columns:
            out[col] = None
    return out


def _load_stg(db_conn, issuer: str | None = None) -> pd.DataFrame:
    sql = """
        SELECT s.*, f.file_name
        FROM stg_834_records s
        LEFT JOIN raw_file_inventory f ON s.file_id = f.file_id
    """
    params: tuple = ()
    if issuer:
        sql += " WHERE s.issuer = ?"
        params = (issuer,)
    return pd.read_sql_query(sql, db_conn, params=params)


def export_assets(db_conn, issuer: str | None = None) -> dict[str, int]:
    """Generate assets/ outputs per partition and issuer rollups."""
    settings.ensure_dirs()
    raw = _load_stg(db_conn, issuer)
    if raw.empty:
        logger.warning("No staging records — skipping assets export")
        return {"partitions": 0, "rollups": 0}

    legacy = _stg_to_legacy_df(raw)
    kpi_builder = KpiBuilder()
    schema_val = SchemaValidator()
    dq_val = DataQualityValidator()
    excel = ExcelExporter()
    xml_exp = XmlExporter()
    sqlite = SqliteLoader()
    dashboard = PlotlyDashboard()

    partitions = 0
    issuer_dfs: dict[str, list[pd.DataFrame]] = {}

    for (iid, year, month), grp in legacy.groupby(["issuer_id", "year", "month"]):
        dirs = _partition_dirs(str(iid), str(year), str(month))
        for d in dirs.values():
            d.mkdir(parents=True, exist_ok=True)
        stem = f"{iid}_{year}_{month}"
        part_df = grp.copy()

        validation_df = dq_val.results_to_dataframe(
            schema_val.validate(part_df, str(iid))
            + dq_val.validate(part_df, str(iid))
        )
        missingness_df = dq_val.build_missingness_df(part_df)
        file_profile_df = dq_val.build_file_profile_df(part_df)
        kpis = kpi_builder.build_kpis(part_df, str(iid))
        kpi_summary_df = kpi_builder.kpis_to_summary_df(kpis)

        excel.export_enrollees(part_df, stem, dirs["excel"])
        excel.export_kpis(kpis, kpi_summary_df, stem, dirs["excel"])
        excel.export_validation_report(
            validation_df, missingness_df, file_profile_df, stem, dirs["excel"]
        )
        xml_exp.export_enrollees(part_df, stem, dirs["cleaned_xml"])
        sqlite.load(part_df, kpi_summary_df, validation_df, stem, dirs["sqlite"])
        dashboard.generate(
            kpis, kpi_summary_df, validation_df, missingness_df,
            stem, dirs["dashboards"],
            title=f"Issuer {iid} — {year}/{month} Dashboard",
        )
        json_path = dirs["validation_reports"] / f"validation_report_{stem}.json"
        json_path.write_text(
            json.dumps(validation_df.to_dict(orient="records"), indent=2),
            encoding="utf-8",
        )
        issuer_dfs.setdefault(str(iid), []).append(part_df)
        partitions += 1
        logger.info("Assets exported: %s/%s/%s", iid, year, month)

    rollups = 0
    for iid, dfs in issuer_dfs.items():
        combined = pd.concat(dfs, ignore_index=True)
        dirs = _rollup_dirs(iid)
        stem = f"{iid}_all_periods"
        validation_df = dq_val.results_to_dataframe(
            schema_val.validate(combined, iid) + dq_val.validate(combined, iid)
        )
        missingness_df = dq_val.build_missingness_df(combined)
        kpis = kpi_builder.build_kpis(combined, iid)
        kpi_summary_df = kpi_builder.kpis_to_summary_df(kpis)

        excel.export_enrollees(combined, stem, dirs["excel"])
        excel.export_kpis(kpis, kpi_summary_df, stem, dirs["excel"], is_rollup=True)
        sqlite.load(combined, kpi_summary_df, validation_df, stem, dirs["sqlite"], rollup=True)
        dashboard.generate(
            kpis, kpi_summary_df, validation_df, missingness_df,
            stem, dirs["dashboards"],
            title=f"Issuer {iid} — All Periods Rollup",
            is_rollup=True,
        )
        rollups += 1
        logger.info("Rollup assets exported: %s", iid)

    return {"partitions": partitions, "rollups": rollups}
