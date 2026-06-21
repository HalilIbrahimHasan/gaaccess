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
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def comparison_output_dir() -> Path:
    d = settings.outputs_path / "comparison"
    d.mkdir(parents=True, exist_ok=True)
    return d


def export_final_comparison(
    comparison: dict[str, Any],
    *,
    issuer: str = "",
) -> dict[str, str]:
    out = comparison_output_dir()
    winner: ComparisonResult | None = comparison.get("winner")
    results: list[ComparisonResult] = comparison.get("results", [])

    score_rows = []
    detail_rows = []
    for r in results:
        row = {
            "strategy_id": r.strategy_id,
            "source_table": r.source_table,
            "date_column": r.date_column,
            "status_column": r.status_column,
            "join_key": r.join_key,
            "overall_accuracy": r.overall_accuracy,
            **r.scores,
        }
        score_rows.append(row)
        detail_rows.extend(r.detail_rows)

    strategy_scores = pd.DataFrame(score_rows)
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

    paths = {}
    paths["comparison_summary"] = str(out / "comparison_summary.xlsx")
    paths["comparison_details"] = str(out / "comparison_details.xlsx")
    paths["strategy_scores"] = str(out / "strategy_scores.xlsx")
    paths["best_strategy"] = str(out / "best_strategy.xlsx")

    write_excel_report(Path(paths["comparison_summary"]), {"summary": summary, "strategy_scores": strategy_scores})
    write_excel_report(Path(paths["comparison_details"]), {"details": comparison_details})
    write_excel_report(Path(paths["strategy_scores"]), {"strategy_scores": strategy_scores})
    write_excel_report(Path(paths["best_strategy"]), {"best_strategy": best})

    sqlite_path = out / "comparison.sqlite"
    with sqlite3.connect(sqlite_path) as conn:
        if not strategy_scores.empty:
            strategy_scores.to_sql("strategy_scores", conn, if_exists="replace", index=False)
        if not comparison_details.empty:
            comparison_details.to_sql("comparison_details", conn, if_exists="replace", index=False)
        if not best.empty:
            best.to_sql("best_strategy", conn, if_exists="replace", index=False)
    paths["comparison_sqlite"] = str(sqlite_path)

    paths["comparison_dashboard"] = str(_write_comparison_dashboard(out, strategy_scores, winner))
    paths["strategy_dashboard"] = str(_write_strategy_dashboard(out, strategy_scores))
    paths["business_dashboard"] = str(_write_business_dashboard(out, winner))

    logger.info("Final comparison exported to %s", out)
    return paths


def _write_comparison_dashboard(out: Path, scores: pd.DataFrame, winner: ComparisonResult | None) -> Path:
    path = out / "comparison_dashboard.html"
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
    return path


def _write_strategy_dashboard(out: Path, scores: pd.DataFrame) -> Path:
    path = out / "strategy_dashboard.html"
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
    return path


def _write_business_dashboard(out: Path, winner: ComparisonResult | None) -> Path:
    path = out / "business_dashboard.html"
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
    return path
