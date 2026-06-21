"""Export final comparison outputs to outputs/comparison/."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from azure_reconciliation.excel_exporter import write_excel_report
from azure_reconciliation.final_comparison_engine import ComparisonResult
from azure_reconciliation.safe_export import (
    ExportErrors,
    safe_write_csv,
    safe_write_html_report,
    safe_write_sqlite,
    write_csv_fallback,
)
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def comparison_output_dir() -> Path:
    d = settings.outputs_path / "comparison"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _strategy_scores_df(results: list[ComparisonResult]) -> pd.DataFrame:
    score_rows = []
    for r in results:
        score_rows.append({
            "strategy_id": r.strategy_id,
            "source_table": r.source_table,
            "date_column": r.date_column,
            "status_column": r.status_column,
            "join_key": r.join_key,
            "overall_accuracy": r.overall_accuracy,
            "behavior_penalty": r.behavior_penalty,
            "behavior_bonus": r.behavior_bonus,
            "behavior_notes": r.behavior_notes,
            **r.scores,
        })
    return pd.DataFrame(score_rows)


def _build_final_result_frames(
    comparison: dict[str, Any],
    *,
    issuer: str = "",
    export_errors: ExportErrors | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    winner: ComparisonResult | None = comparison.get("winner")
    results: list[ComparisonResult] = comparison.get("results", [])
    rejected: list[dict[str, str]] = comparison.get("rejected", [])

    strategy_scores = _strategy_scores_df(results)
    detail_rows: list[dict] = []
    for r in results:
        detail_rows.extend(r.detail_rows)
    comparison_details = pd.DataFrame(detail_rows)

    if winner:
        reliable = comparison.get("accuracy_reliable", comparison.get("match_count", 0) > 0)
        best = pd.DataFrame([{
            "issuer": issuer,
            "selected_table": winner.source_table,
            "selected_strategy": winner.strategy_id,
            "selected_date_column": winner.date_column,
            "selected_status_column": winner.status_column,
            "selected_join_key": winner.join_key,
            "auto_join_mapping": comparison.get("join_mapping", ""),
            "selected_status_mapping": str(winner.status_mapping) if reliable else "not reliable",
            "status_mapping_reliable": comparison.get("status_mapping_reliable", reliable),
            "accuracy_reliable": reliable,
            "match_count": comparison.get("match_count", 0),
            "confidence_score": winner.overall_accuracy if reliable else None,
            "overall_accuracy": winner.overall_accuracy,
            "overall_accuracy_note": "" if reliable else "NOT RELIABLE — 0 record matches; fix ID mapping",
            "historical_window": comparison.get("historical_window", ""),
            "xml_raw_rows": comparison.get("xml_raw_rows"),
            "xml_lifecycle_rows": comparison.get("xml_lifecycle_rows"),
            "weakest_level": min(winner.scores, key=winner.scores.get) if winner.scores else "",
            "weakest_score": min(winner.scores.values()) if winner.scores else None,
            **winner.scores,
        }])
    else:
        best = pd.DataFrame([{
            "issuer": issuer,
            "cannot_calculate_reason": comparison.get("cannot_calculate_reason", ""),
            "xml_raw_rows": comparison.get("xml_raw_rows"),
            "xml_lifecycle_rows": comparison.get("xml_lifecycle_rows"),
        }])

    rejected_df = pd.DataFrame(rejected) if rejected else pd.DataFrame()
    if export_errors and export_errors.errors:
        err_df = pd.DataFrame({"export_error": export_errors.errors})
        rejected_df = pd.concat([rejected_df, err_df], ignore_index=True)

    summary = best.copy()
    return summary, strategy_scores, pd.concat(
        [comparison_details, rejected_df], ignore_index=True,
    ) if not comparison_details.empty or not rejected_df.empty else comparison_details


def export_final_result(
    comparison: dict[str, Any],
    *,
    issuer: str = "",
    export_errors: ExportErrors | None = None,
) -> dict[str, str]:
    """Write outputs/comparison/final_result.{html,xlsx,csv}."""
    out = comparison_output_dir()
    summary, strategy_scores, details = _build_final_result_frames(
        comparison, issuer=issuer, export_errors=export_errors,
    )
    winner: ComparisonResult | None = comparison.get("winner")
    rejected = comparison.get("rejected", [])
    paths: dict[str, str] = {}

    # CSV
    csv_path = out / "final_result.csv"
    safe_write_csv(csv_path, summary, table_name="final_result", export_errors=export_errors)
    paths["final_result_csv"] = str(csv_path)
    write_csv_fallback("final_result_strategy_scores", strategy_scores, export_errors=export_errors)

    # Excel
    xlsx_path = out / "final_result.xlsx"
    result = write_excel_report(
        xlsx_path,
        {
            "final_result": summary,
            "strategy_scores": strategy_scores,
            "comparison_details": details,
            "rejected_strategies": pd.DataFrame(rejected),
        },
        export_errors=export_errors,
    )
    if result:
        paths["final_result_xlsx"] = str(xlsx_path)

    # HTML
    extra = ""
    if winner:
        reliable = comparison.get("accuracy_reliable", comparison.get("match_count", 0) > 0)
        weak = min(winner.scores, key=winner.scores.get) if winner.scores else "n/a"
        extra = (
            f"<p><strong>Selected:</strong> {winner.source_table} / Strategy {winner.strategy_id}</p>"
            f"<p><strong>Auto join:</strong> {comparison.get('join_mapping', winner.join_key)}</p>"
            f"<p><strong>Record matches:</strong> {comparison.get('match_count', 0)}</p>"
        )
        if not reliable:
            extra += (
                "<p style='color:#c00'><strong>RECORD MATCH = 0 — accuracy is NOT reliable.</strong></p>"
                "<p>Next action: review <code>outputs/debug/id_overlap_matrix.csv</code> "
                "and correct ID column mapping.</p>"
                "<p><strong>Status mapping:</strong> not reliable (suppressed)</p>"
            )
        else:
            extra += (
                f"<p><strong>Overall accuracy:</strong> {winner.overall_accuracy:.1f}%</p>"
                f"<p><strong>Weakest level:</strong> {weak}</p>"
            )
    else:
        extra = f"<p><strong>Cannot calculate:</strong> {comparison.get('cannot_calculate_reason', 'unknown')}</p>"

    html_path = out / "final_result.html"
    safe_write_html_report(
        html_path,
        title=f"Final Result — issuer {issuer}",
        summary_df=summary,
        detail_df=strategy_scores,
        extra_html=extra,
        export_errors=export_errors,
    )
    paths["final_result_html"] = str(html_path)
    return paths


def export_final_comparison(
    comparison: dict[str, Any],
    *,
    issuer: str = "",
    export_errors: ExportErrors | None = None,
) -> dict[str, str]:
    out = comparison_output_dir()
    winner: ComparisonResult | None = comparison.get("winner")
    results: list[ComparisonResult] = comparison.get("results", [])

    strategy_scores = _strategy_scores_df(results)
    detail_rows: list[dict] = []
    for r in results:
        detail_rows.extend(r.detail_rows)
    comparison_details = pd.DataFrame(detail_rows)

    if winner:
        best = pd.DataFrame([{
            "selected_table": winner.source_table,
            "selected_strategy": winner.strategy_id,
            "selected_date_column": winner.date_column,
            "selected_status_column": winner.status_column,
            "selected_join_key": winner.join_key,
            "selected_status_mapping": str(winner.status_mapping),
            "confidence_score": winner.overall_accuracy,
            "overall_accuracy": winner.overall_accuracy,
            "historical_window": comparison.get("historical_window", ""),
            **winner.scores,
        }])
    else:
        best = pd.DataFrame()

    summary = best.copy()
    if not summary.empty:
        summary["issuer"] = issuer

    paths: dict[str, str] = {}
    paths["comparison_summary"] = str(out / "comparison_summary.xlsx")
    paths["comparison_details"] = str(out / "comparison_details.xlsx")
    paths["strategy_scores"] = str(out / "strategy_scores.xlsx")
    paths["best_strategy"] = str(out / "best_strategy.xlsx")

    write_excel_report(
        Path(paths["comparison_summary"]),
        {"summary": summary, "strategy_scores": strategy_scores},
        export_errors=export_errors,
    )
    write_excel_report(
        Path(paths["comparison_details"]),
        {"details": comparison_details},
        export_errors=export_errors,
    )
    write_excel_report(
        Path(paths["strategy_scores"]),
        {"strategy_scores": strategy_scores},
        export_errors=export_errors,
    )
    write_excel_report(
        Path(paths["best_strategy"]),
        {"best_strategy": best},
        export_errors=export_errors,
    )

    sqlite_path = out / "comparison.sqlite"
    try:
        with sqlite3.connect(sqlite_path) as conn:
            if not strategy_scores.empty:
                safe_write_sqlite(conn, "strategy_scores", strategy_scores, export_errors=export_errors)
            if not comparison_details.empty:
                safe_write_sqlite(conn, "comparison_details", comparison_details, export_errors=export_errors)
            if not best.empty:
                safe_write_sqlite(conn, "best_strategy", best, export_errors=export_errors)
    except Exception as exc:
        if export_errors:
            export_errors.record(f"comparison.sqlite failed: {exc}")

    paths["comparison_sqlite"] = str(sqlite_path)
    write_csv_fallback("strategy_scores", strategy_scores, export_errors=export_errors)
    write_csv_fallback("comparison_details", comparison_details, export_errors=export_errors)

    paths["comparison_dashboard"] = str(_write_comparison_dashboard(out, strategy_scores, winner))
    paths["strategy_dashboard"] = str(_write_strategy_dashboard(out, strategy_scores))
    paths["business_dashboard"] = str(_write_business_dashboard(out, winner))

    paths.update(export_final_result(comparison, issuer=issuer, export_errors=export_errors))
    logger.info("Final comparison exported to %s", out)
    return paths


def _write_comparison_dashboard(out: Path, scores: pd.DataFrame, winner: ComparisonResult | None) -> Path:
    path = out / "comparison_dashboard.html"
    try:
        fig = make_subplots(rows=1, cols=2, subplot_titles=("Strategy Accuracy", "Winner Scores"))
        if not scores.empty:
            fig.add_trace(
                go.Bar(x=scores["source_table"] + " / " + scores["strategy_id"], y=scores["overall_accuracy"]),
                row=1, col=1,
            )
        if winner:
            fig.add_trace(
                go.Bar(x=list(winner.scores.keys()), y=list(winner.scores.values()), name="Winner"),
                row=1, col=2,
            )
        fig.update_layout(title="Final Comparison Dashboard", template="plotly_dark", height=500)
        fig.write_html(str(path), include_plotlyjs="cdn")
    except Exception as exc:
        logger.warning("comparison_dashboard.html failed: %s", exc)
        path.write_text(f"<html><body><p>Dashboard failed: {exc}</p></body></html>")
    return path


def _write_strategy_dashboard(out: Path, scores: pd.DataFrame) -> Path:
    path = out / "strategy_dashboard.html"
    try:
        fig = go.Figure()
        if not scores.empty:
            for sid in scores["strategy_id"].unique():
                sub = scores[scores["strategy_id"] == sid]
                fig.add_trace(go.Scatter(
                    x=sub["source_table"], y=sub["overall_accuracy"],
                    mode="markers+lines", name=f"Strategy {sid}",
                ))
        fig.update_layout(title="Strategy Scores by Table", template="plotly_dark", height=450)
        fig.write_html(str(path), include_plotlyjs="cdn")
    except Exception as exc:
        path.write_text(f"<html><body><p>Dashboard failed: {exc}</p></body></html>")
    return path


def _write_business_dashboard(out: Path, winner: ComparisonResult | None) -> Path:
    path = out / "business_dashboard.html"
    try:
        fig = go.Figure()
        if winner:
            fig.add_trace(go.Bar(
                x=list(winner.scores.keys()),
                y=list(winner.scores.values()),
                text=[f"{v:.1f}%" for v in winner.scores.values()],
                textposition="auto",
            ))
        fig.update_layout(
            title=f"Business Accuracy — {winner.source_table if winner else 'n/a'}",
            template="plotly_dark", height=450,
        )
        fig.write_html(str(path), include_plotlyjs="cdn")
    except Exception as exc:
        path.write_text(f"<html><body><p>Dashboard failed: {exc}</p></body></html>")
    return path
