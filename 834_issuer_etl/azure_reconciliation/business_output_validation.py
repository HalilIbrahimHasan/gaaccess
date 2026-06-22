"""
Business output validation — audits and documentation only.

Does NOT change transformation logic; documents metrics and investigates IDs.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.df_utils import find_col, normalize_id_series
from azure_reconciliation.partition_discovery import Partition
from azure_reconciliation.reconciliation_analysis import (
    XML_ENROLLMENT_ID_COLS,
    XML_SUBSCRIBER_ID_COLS,
)
from azure_reconciliation.safe_export import safe_write_excel
from config.config import settings
from utils.logger import get_logger

if True:
    from azure_reconciliation.xml_business_reports import IssuerBusinessResult

logger = get_logger(__name__)

_SUBSCRIBER_COL_PAT = re.compile(r"subscriber", re.I)


def _debug_dir() -> Path:
    d = settings.outputs_path / "debug"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _model_h_subscriber_candidates() -> list[str]:
    return list(XML_SUBSCRIBER_ID_COLS)


def _discover_subscriber_columns(df: pd.DataFrame) -> list[str]:
    if df is None or df.empty:
        return []
    return sorted(c for c in df.columns if _SUBSCRIBER_COL_PAT.search(str(c)))


def _nonempty_stats(series: pd.Series) -> tuple[int, int, str]:
    normed = normalize_id_series(series)
    valid = normed[normed.astype(str).str.strip() != ""]
    samples = valid.drop_duplicates().head(5).tolist()
    return int(valid.shape[0]), int(valid.nunique()), "; ".join(str(s) for s in samples)


def _used_by_model_h(col_name: str, lifecycle_input: pd.DataFrame) -> str:
    candidates = _model_h_subscriber_candidates()
    matched = find_col(
        pd.DataFrame(columns=list(lifecycle_input.columns)),
        col_name,
        *[c for c in candidates if c.lower() == col_name.lower()],
    )
    if not matched:
        for cand in candidates:
            if cand.lower() == col_name.lower():
                matched = cand if cand in lifecycle_input.columns else find_col(lifecycle_input, cand)
                break
    if not matched or matched not in lifecycle_input.columns:
        return "No"
    nonempty, _, _ = _nonempty_stats(lifecycle_input[matched])
    return "Yes" if nonempty > 0 else "No (column absent or empty on Model H input)"


def build_subscriber_mapping_audit(
    xml_raw: pd.DataFrame,
    canonical: pd.DataFrame,
    lifecycle_input: pd.DataFrame,
    *,
    issuer: str = "",
) -> pd.DataFrame:
    """Audit every subscriber-like column across XML → canonical → Model H input."""
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    sources = (
        ("xml_raw", xml_raw),
        ("canonical", canonical),
        ("lifecycle_input", lifecycle_input),
    )
    for source_label, df in sources:
        for col in _discover_subscriber_columns(df):
            key = col.lower()
            if key in seen and source_label != "xml_raw":
                continue
            nonempty, distinct, samples = _nonempty_stats(df[col])
            rows.append({
                "issuer": issuer,
                "source_frame": source_label,
                "column_name": col,
                "non_null_count": nonempty,
                "distinct_count": distinct,
                "sample_values": samples,
                "used_by_model_h": _used_by_model_h(col, lifecycle_input),
                "model_h_candidate_list": ";".join(_model_h_subscriber_candidates()),
            })
            seen.add(key)

    for cand in _model_h_subscriber_candidates():
        if cand.lower() in seen:
            continue
        col = find_col(canonical, cand) or find_col(xml_raw, cand)
        if col:
            continue
        rows.append({
            "issuer": issuer,
            "source_frame": "(not present)",
            "column_name": cand,
            "non_null_count": 0,
            "distinct_count": 0,
            "sample_values": "",
            "used_by_model_h": "No",
            "model_h_candidate_list": ";".join(_model_h_subscriber_candidates()),
        })

    return pd.DataFrame(rows)


def build_subscriber_mapping_audit_all(results: list[IssuerBusinessResult]) -> pd.DataFrame:
    frames = [
        build_subscriber_mapping_audit(
            r.xml_raw, r.canonical, r.lifecycle_input, issuer=r.issuer,
        )
        for r in results
    ]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _filter_partition_df(
    df: pd.DataFrame,
    part: Partition,
    *,
    kind: str = "canonical",
) -> pd.DataFrame:
    if df.empty:
        return df
    if kind == "xml_raw":
        mask = df.get("issuer", pd.Series(dtype=str)).astype(str) == str(part.issuer)
        if "year" in df.columns:
            mask &= df["year"].astype(str) == str(part.year)
        if "month" in df.columns:
            mask &= df["month"].astype(str).str.zfill(2) == str(part.month).zfill(2)
        return df[mask].copy()
    mask = df.get("issuer", pd.Series(dtype=str)).astype(str) == str(part.issuer)
    if "year" in df.columns:
        mask &= df["year"].astype(str) == str(part.year)
    if "month" in df.columns:
        mask &= df["month"].astype(str).str.zfill(2) == str(part.month).zfill(2)
    return df[mask].copy()


def build_business_processing_summary(
    result: IssuerBusinessResult,
    partition: Partition | None = None,
) -> pd.DataFrame:
    """Dynamic processing totals for report headers."""
    if partition:
        raw = _filter_partition_df(result.xml_raw, partition, kind="xml_raw")
        canon = _filter_partition_df(result.canonical, partition)
        life = _filter_partition_df(result.lifecycle_input, partition)
        dup = _filter_partition_df(result.duplicate_df, partition)
        maint = _filter_partition_df(result.maintenance_df, partition)
        sup = _filter_partition_df(result.superseded_df, partition)
        monthly = result.business_monthly
        if not monthly.empty:
            monthly = monthly[
                (monthly["year"].astype(str) == str(partition.year))
                & (monthly["month"].astype(str).str.zfill(2) == str(partition.month).zfill(2))
            ]
    else:
        raw, canon, life = result.xml_raw, result.canonical, result.lifecycle_input
        dup, maint, sup = result.duplicate_df, result.maintenance_df, result.superseded_df
        monthly = result.business_monthly

    month_basis = ""
    if not result.month_basis_audit.empty:
        top = result.month_basis_audit.sort_values("row_count", ascending=False).iloc[0]
        month_basis = f"{top['month_basis_used']} ({top['pct']}% of canonical rows)"

    scope = partition.label() if partition else f"issuer {result.issuer} (all partitions)"

    return pd.DataFrame([{
        "scope": scope,
        "raw_xml_rows": len(raw),
        "canonical_rows": len(canon),
        "duplicate_transactions_removed": len(dup),
        "maintenance_only_events": len(maint),
        "superseded_events": len(sup),
        "latest_state_records": len(life),
        "dashboard_groups": len(monthly),
        "month_basis_used": month_basis,
    }])


def processing_summary_html(summary_df: pd.DataFrame) -> str:
    if summary_df.empty:
        return ""
    row = summary_df.iloc[0]
    labels = {
        "raw_xml_rows": "Raw XML Rows",
        "canonical_rows": "Canonical Rows",
        "duplicate_transactions_removed": "Duplicate Transactions Removed",
        "maintenance_only_events": "Maintenance-only Events",
        "superseded_events": "Superseded Events",
        "latest_state_records": "Latest State Records (Model H input)",
        "dashboard_groups": "Dashboard Groups",
        "month_basis_used": "Month Basis Used",
    }
    lines = ["<h2>Business Processing Summary</h2>", "<table>", "<tr><th>Metric</th><th>Value</th></tr>"]
    for key, label in labels.items():
        if key in row.index:
            lines.append(f"<tr><td>{label}</td><td>{row[key]}</td></tr>")
    lines.append("</table>")
    return "\n".join(lines)


def lifecycle_snapshot_export_df(snapshots: pd.DataFrame) -> pd.DataFrame:
    """Export-only rename for clarity; does not change replay calculations."""
    if snapshots.empty:
        return snapshots
    out = snapshots.copy()
    if "event_count" in out.columns and "lifecycle_replayed_event_count" not in out.columns:
        out = out.rename(columns={"event_count": "lifecycle_replayed_event_count"})
    return out


def write_subscriber_mapping_audit(
    results: list[IssuerBusinessResult],
) -> Path:
    path = _debug_dir() / "subscriber_mapping_audit.xlsx"
    audit = build_subscriber_mapping_audit_all(results)
    findings = pd.DataFrame()
    if not audit.empty:
        canon_has = audit[
            (audit["source_frame"] == "canonical") & (audit["non_null_count"] > 0)
        ]
        life_used = audit[audit["used_by_model_h"].astype(str).str.startswith("Yes")]
        findings = pd.DataFrame([{
            "finding": "subscriber_ids_in_canonical",
            "count": len(canon_has),
            "detail": "; ".join(canon_has["column_name"].tolist()) if not canon_has.empty else "none",
        }, {
            "finding": "subscriber_cols_on_model_h_input",
            "count": len(life_used),
            "detail": "; ".join(life_used["column_name"].tolist()) if not life_used.empty else "none",
        }])
    safe_write_excel(
        path,
        {"subscriber_mapping_audit": audit, "findings": findings},
        drop_duplicate_value_columns=False,
    )
    logger.info("Wrote subscriber mapping audit → %s", path)
    return path


def write_business_validation_md(results: list[IssuerBusinessResult]) -> Path:
    path = _debug_dir() / "business_validation.md"
    audit = build_subscriber_mapping_audit_all(results)
    generated = datetime.now(timezone.utc).isoformat(timespec="seconds")
    issuers = ", ".join(r.issuer for r in results)

    event_dist: dict[int, int] = {}
    for r in results:
        if not r.lifecycle_snapshots.empty and "event_count" in r.lifecycle_snapshots.columns:
            for k, v in r.lifecycle_snapshots["event_count"].value_counts().items():
                event_dist[int(k)] = event_dist.get(int(k), 0) + int(v)

    lines = [
        "# Business Validation Reference",
        "",
        f"**Generated:** {generated}",
        f"**Issuers audited:** {issuers}",
        "",
        "This document describes report metrics only. Calculations are unchanged.",
        "",
        "## Enrollment_Count",
        "",
        "Distinct canonical enrollment/policy IDs per dashboard group.",
        "",
        "Model H coalesces the first non-empty value per row across candidate columns",
        f"defined in `XML_ENROLLMENT_ID_COLS` ({', '.join(XML_ENROLLMENT_ID_COLS)}),",
        "then counts unique non-empty values per group:",
        "`issuer`, `year`, `month`, `insurance_type`, `status`.",
        "",
        "## Enrollee_Count",
        "",
        "Distinct canonical member/enrollee IDs per dashboard group.",
        "Same coalescing pattern as enrollment using `XML_ENROLLEE_ID_COLS`.",
        "",
        "## Subscriber_Count",
        "",
        "Distinct canonical subscriber IDs per dashboard group.",
        "Coalescing uses `XML_SUBSCRIBER_ID_COLS`:",
        f"`{'`, `'.join(_model_h_subscriber_candidates())}`.",
        "",
        "### Subscriber investigation evidence",
        "",
    ]

    if not audit.empty:
        for _, row in audit.iterrows():
            lines.append(
                f"- **{row['column_name']}** ({row['source_frame']}): "
                f"non-null={row['non_null_count']}, distinct={row['distinct_count']}, "
                f"Model H input={row['used_by_model_h']}, samples={row['sample_values'][:80]}"
            )
    else:
        lines.append("- No subscriber columns discovered.")

    lines.extend([
        "",
        "Subscriber IDs must be present on the **Model H input** frame (`lifecycle_input`).",
        "Lifecycle replay output does not emit subscriber columns; they are attached from",
        "canonical records on the lifecycle primary join key before Model H runs.",
        "",
        "## Canonical_Row_Count",
        "",
        "Count of canonical XML transaction rows matching the dashboard group grain",
        "(issuer, business year, business month, insurance_type, status).",
        "One row per parsed XML transaction after canonical normalization.",
        "",
        "## Latest_State_Record_Count (formerly Lifecycle_Row_Count)",
        "",
        "Count of **latest-state records** in the Model H input frame for this group.",
        "These are lifecycle-replayed member states (one per member key per partition month),",
        "not raw XML event rows. This number can differ from Canonical_Row_Count because:",
        "",
        "- Canonical counts raw transactions; latest-state counts replay output rows.",
        "- Business month basis may assign rows to different groups than source folder month.",
        "- Multiple partitions can contribute latest-state rows to the same business month group.",
        "",
        "**Calculation unchanged** — only the column label was clarified.",
        "",
        "## Duplicate_Count",
        "",
        "Canonical rows identified as duplicate transactions (same lifecycle primary key,",
        "status, and effective dates) where a later row is kept.",
        "",
        "## Maintenance_Only_Count",
        "",
        "Canonical rows whose maintenance action code prefix matches maintenance-only codes",
        "in `MAINT_ACTION_PREFIXES` (reconciliation_analysis.py).",
        "",
        "## Superseded_Count",
        "",
        "Canonical rows that are not the chronologically final row for their lifecycle primary key.",
        "",
        "## Month_Basis_Used",
        "",
        "Which business month column was selected per row, in priority order:",
        "1. `file_event_year_month` 2. `member_maint_year_month`",
        "3. `benefit_effective_year_month` 4. `coverage_year_month` (fallback).",
        "Dashboard `year`/`month` dimensions use this business month.",
        "",
        "## lifecycle_replayed_event_count (exported from lifecycle snapshots)",
        "",
        "Renamed from `event_count` in exported CSV files for clarity only.",
        "",
        "Represents the number of XML transactions replayed for the member lifecycle key",
        "up to and including the snapshot partition month (`replay_lifecycle` in lifecycle_engine.py).",
        "Increments once per XML event row processed during replay.",
        "",
        f"**Observed distribution across audited issuers:** `{event_dist or 'no data'}`",
        "",
        "A value of `1` means exactly one XML transaction was replayed for that member key",
        "through that month — this is expected when members have a single transaction.",
        "",
    ])

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("Wrote business validation doc → %s", path)
    return path
