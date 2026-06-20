"""Azure query diagnostics before mirror report generation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy.engine import Engine

from azure_reconciliation.azure_client import _azure_settings
from azure_reconciliation.azure_mirror.columns import (
    log_missing_columns,
    resolve_column,
)
from azure_reconciliation.azure_mirror.query import (
    DIAGNOSTIC_DATE_COLUMNS,
    MONTH_SOURCE_PRIORITY,
    fetch_enrollments_issuer_year,
    filter_by_month,
    resolve_month_source_column,
)
from azure_reconciliation.partition_discovery import Partition
from utils.logger import get_logger

logger = get_logger(__name__)


def _distinct_values(df: pd.DataFrame, canonical: str) -> list[str]:
    actual = resolve_column(list(df.columns), canonical)
    if not actual or actual not in df.columns or df.empty:
        return []
    return sorted(df[actual].dropna().astype(str).unique().tolist())


def _date_min_max(df: pd.DataFrame, canonical: str) -> dict[str, Any]:
    actual = resolve_column(list(df.columns), canonical)
    if not actual or actual not in df.columns or df.empty:
        return {
            "column": canonical,
            "actual_column": actual or "",
            "present": bool(actual),
            "min": None,
            "max": None,
            "non_null_count": 0,
        }
    dates = pd.to_datetime(df[actual], errors="coerce")
    valid = dates.dropna()
    return {
        "column": canonical,
        "actual_column": actual,
        "present": True,
        "min": str(valid.min()) if not valid.empty else None,
        "max": str(valid.max()) if not valid.empty else None,
        "non_null_count": int(valid.shape[0]),
    }


def _month_counts_for_column(df: pd.DataFrame, canonical: str) -> pd.DataFrame:
    actual = resolve_column(list(df.columns), canonical)
    if not actual or actual not in df.columns or df.empty:
        return pd.DataFrame(columns=["date_column", "year", "month", "row_count"])

    dates = pd.to_datetime(df[actual], errors="coerce")
    tmp = pd.DataFrame({"_yr": dates.dt.year, "_mo": dates.dt.month})
    tmp = tmp.dropna()
    if tmp.empty:
        return pd.DataFrame(columns=["date_column", "year", "month", "row_count"])

    counts = tmp.groupby(["_yr", "_mo"], dropna=False).size().reset_index(name="row_count")
    counts["date_column"] = canonical
    counts["year"] = counts["_yr"].astype(int)
    counts["month"] = counts["_mo"].astype(int).astype(str).str.zfill(2)
    return counts[["date_column", "year", "month", "row_count"]]


def log_partition_diagnostics(
    *,
    table: str,
    sql: str,
    params: dict[str, Any],
    df: pd.DataFrame,
    issuer: str,
    year: str,
    month: str,
    month_filtered_count: int | None = None,
    month_column: str | None = None,
) -> None:
    logger.info("Azure connection successful")
    logger.info("Azure table used: %s", table)
    logger.info("Azure query SQL: %s", sql)
    logger.info("Azure query parameters: %s", params)
    logger.info("Azure raw row count returned: %d", len(df))
    logger.info(
        "Azure distinct hios_issuer_id: %s",
        _distinct_values(df, "hios_issuer_id") or "(none)",
    )
    logger.info(
        "Azure distinct coverage_year: %s",
        _distinct_values(df, "coverage_year") or "(none)",
    )
    for col in DIAGNOSTIC_DATE_COLUMNS:
        mm = _date_min_max(df, col)
        if mm["present"]:
            logger.info(
                "Azure date column %s min=%s max=%s non_null=%s",
                col, mm["min"], mm["max"], mm["non_null_count"],
            )
    if month_filtered_count is not None:
        logger.info(
            "Azure month filter %s/%s via %s: %d rows",
            year, month, month_column or "(none)", month_filtered_count,
        )


def run_issuer_diagnostics(
    engine: Engine,
    *,
    issuer: str,
    partitions: list[Partition],
    table_columns: list[str],
) -> dict[str, Any]:
    cfg = _azure_settings()
    schema, table = cfg["schema"], cfg["table"]
    full_table = f"[{schema}].[{table}]"
    missing = log_missing_columns(table_columns, context=full_table)

    years = sorted({p.year for p in partitions})
    year_data: dict[str, pd.DataFrame] = {}
    year_queries: dict[str, tuple[str, dict[str, Any]]] = {}
    query_log: list[dict[str, Any]] = []

    for year in years:
        df, sql, params, tbl = fetch_enrollments_issuer_year(
            engine,
            issuer=issuer,
            year=year,
            schema=schema,
            table=table,
            columns=table_columns,
        )
        year_data[year] = df
        year_queries[year] = (sql, params)
        query_log.append({
            "issuer": issuer,
            "year": year,
            "month": "(all)",
            "table": tbl,
            "sql": sql,
            "params": str(params),
            "raw_row_count": len(df),
            "month_filtered_count": len(df),
            "month_column_used": "",
            "note": "issuer+year base query (no SQL month filter)",
        })

    combined = (
        pd.concat([year_data[y] for y in years if not year_data[y].empty], ignore_index=True)
        if any(not year_data[y].empty for y in years)
        else pd.DataFrame()
    )

    month_column = resolve_month_source_column(table_columns, combined)
    if month_column:
        logger.info("Azure month source column selected: %s", month_column)
    else:
        logger.warning("No month source column found in Azure table")

    partition_frames: dict[str, pd.DataFrame] = {}
    for part in partitions:
        base = year_data.get(part.year, pd.DataFrame())
        sql, params = year_queries.get(part.year, ("", {}))

        if base.empty:
            filtered = pd.DataFrame()
        elif month_column:
            filtered = filter_by_month(
                base, year=part.year, month=part.month, month_column_canonical=month_column
            )
        else:
            filtered = base.copy()

        partition_frames[part.label()] = filtered

        log_partition_diagnostics(
            table=full_table,
            sql=sql,
            params=params,
            df=base,
            issuer=issuer,
            year=part.year,
            month=part.month,
            month_filtered_count=len(filtered),
            month_column=month_column,
        )

        query_log.append({
            "issuer": part.issuer,
            "year": part.year,
            "month": part.month,
            "table": full_table,
            "sql": sql,
            "params": str(params),
            "raw_row_count": len(base),
            "month_filtered_count": len(filtered),
            "month_column_used": month_column or "",
            "note": "Python month filter on issuer+year base",
        })

    raw_sample = combined.head(500) if not combined.empty else pd.DataFrame({"note": ["no rows"]})
    row_counts_by_year = pd.DataFrame([{"year": y, "row_count": len(year_data[y])} for y in years])

    if not combined.empty:
        issuer_col = resolve_column(list(combined.columns), "hios_issuer_id")
        if issuer_col:
            row_counts_by_issuer = (
                combined.groupby(issuer_col).size().reset_index(name="row_count")
                .rename(columns={issuer_col: "hios_issuer_id"})
            )
        else:
            row_counts_by_issuer = pd.DataFrame({"hios_issuer_id": [issuer], "row_count": [len(combined)]})
    else:
        row_counts_by_issuer = pd.DataFrame(columns=["hios_issuer_id", "row_count"])

    date_min_max = pd.DataFrame([_date_min_max(combined, col) for col in DIAGNOSTIC_DATE_COLUMNS])
    month_count_frames = [_month_counts_for_column(combined, col) for col in DIAGNOSTIC_DATE_COLUMNS]
    month_counts = (
        pd.concat([f for f in month_count_frames if not f.empty], ignore_index=True)
        if any(not f.empty for f in month_count_frames)
        else pd.DataFrame(columns=["date_column", "year", "month", "row_count"])
    )

    missing_df = (
        pd.DataFrame({"missing_column": missing})
        if missing
        else pd.DataFrame({"note": ["all required columns present"]})
    )

    diagnostic_sheets = {
        "raw_sample": raw_sample,
        "row_counts_by_year": row_counts_by_year,
        "row_counts_by_issuer": row_counts_by_issuer,
        "date_min_max": date_min_max,
        "month_counts_by_each_date_column": month_counts,
        "missing_columns": missing_df,
        "query_log": pd.DataFrame(query_log),
        "month_source_priority": pd.DataFrame({
            "priority": range(1, len(MONTH_SOURCE_PRIORITY) + 1),
            "column": MONTH_SOURCE_PRIORITY,
            "selected": [c == month_column for c in MONTH_SOURCE_PRIORITY],
        }),
    }

    return {
        "year_data": year_data,
        "partition_frames": partition_frames,
        "combined": combined,
        "month_column": month_column,
        "diagnostic_sheets": diagnostic_sheets,
        "full_table": full_table,
    }


def write_query_diagnostic_workbook(path: Path, sheets: dict[str, pd.DataFrame]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, frame in sheets.items():
            export = frame if not frame.empty else pd.DataFrame({"note": ["no data"]})
            export.to_excel(writer, sheet_name=name[:31], index=False)
    logger.info("Azure query diagnostic written: %s", path)
    return path


def write_no_data_html(
    *,
    issuer: str,
    partitions: list[Partition],
    output_path: Path,
    diagnostic_xlsx: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    parts_html = "".join(f"<li>{p.issuer}/{p.year}/{p.month}</li>" for p in partitions)
    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Azure — No Data — {issuer}</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; background: #121212; color: #eee; }}
a {{ color: #6eb6ff; }}
</style></head><body>
<h1>No Azure rows returned</h1>
<p>Issuer <strong>{issuer}</strong> — no rows for source_data partitions:</p>
<ul>{parts_html}</ul>
<p>See <strong>azure_query_diagnostic_{issuer}.xlsx</strong> under azurevs/excel/</p>
<p>Check sheets <code>month_counts_by_each_date_column</code> and <code>query_log</code>
if issuer+year rows exist but month-filtered counts are zero.</p>
</body></html>"""
    output_path.write_text(html, encoding="utf-8")
    logger.warning("Azure mirror: no data — wrote %s", output_path)
