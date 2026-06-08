"""
Plotly dashboard generator — interactive HTML dashboards per partition or rollup.

Combines KPI summaries, enrollee distributions, premium analytics, and
validation findings into a single self-contained HTML file.
"""

from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from utils.logger import get_logger

logger = get_logger(__name__)


class PlotlyDashboard:
    """
    Build multi-chart Plotly HTML dashboards for monthly partitions or rollups.

    Monthly dashboards label issuer/year/month explicitly; rollup dashboards
    emphasize trends across ``source_period`` values.
    """

    def generate(
        self,
        kpis: dict[str, Any],
        kpi_summary_df: pd.DataFrame,
        validation_df: pd.DataFrame,
        missingness_df: pd.DataFrame,
        output_stem: str,
        output_dir: Path,
        *,
        title: str,
        is_rollup: bool = False,
    ) -> Path:
        """
        Generate an interactive HTML dashboard file.

        Args:
            kpis: Full KPI dict from ``KpiBuilder``.
            kpi_summary_df: Scalar KPI summary table.
            validation_df: Validation check results.
            missingness_df: Column missingness profile.
            output_stem: Filename stem for the dashboard file.
            output_dir: Target dashboards directory.
            title: Dashboard title shown at the top of the page.
            is_rollup: When True, chart layout favors cross-period trends.

        Returns:
            Path to the generated HTML dashboard.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / f"issuer_{output_stem}_dashboard.html"

        second_bar_title = (
            "Enrollees by Source Period" if is_rollup else "Enrollees by Source File"
        )

        fig = make_subplots(
            rows=4,
            cols=2,
            subplot_titles=(
                "KPI Summary",
                second_bar_title,
                "Subscribers vs Dependents",
                "Premium by Rating Area",
                "Members by Effective Month",
                "Validation Issue Summary",
                "Missingness by Column (Top 15)",
                "Duplicate Count Summary",
            ),
            specs=[
                [{"type": "table"}, {"type": "bar"}],
                [{"type": "pie"}, {"type": "bar"}],
                [{"type": "bar"}, {"type": "bar"}],
                [{"type": "bar"}, {"type": "indicator"}],
            ],
            vertical_spacing=0.08,
            horizontal_spacing=0.06,
        )

        self._add_kpi_table(fig, kpi_summary_df, row=1, col=1)
        if is_rollup:
            self._add_enrollees_by_period(fig, kpis, row=1, col=2)
        else:
            self._add_enrollees_by_file(fig, kpis, row=1, col=2)
        self._add_subscriber_pie(fig, kpis, row=2, col=1)
        self._add_premium_by_rating(fig, kpis, row=2, col=2)
        if is_rollup:
            self._add_premium_by_period(fig, kpis, row=3, col=1)
        else:
            self._add_effective_month(fig, kpis, row=3, col=1)
        self._add_validation_summary(fig, validation_df, row=3, col=2)
        self._add_missingness(fig, missingness_df, row=4, col=1)
        self._add_duplicate_indicator(fig, kpis, row=4, col=2)

        fig.update_layout(
            title_text=title,
            height=1600,
            showlegend=False,
            template="plotly_white",
        )

        fig.write_html(path, include_plotlyjs="cdn", full_html=True)
        logger.info("Generated dashboard: %s", path)
        return path

    def _add_kpi_table(
        self, fig: go.Figure, kpi_df: pd.DataFrame, row: int, col: int
    ) -> None:
        """Add KPI summary as an interactive table."""
        if kpi_df.empty:
            kpi_df = pd.DataFrame({"metric": ["No data"], "value": [0]})
        fig.add_trace(
            go.Table(
                header=dict(
                    values=["Metric", "Value"],
                    fill_color="#4472C4",
                    font=dict(color="white"),
                ),
                cells=dict(
                    values=[kpi_df["metric"], kpi_df["value"].astype(str)],
                    fill_color="#E9EDF4",
                ),
            ),
            row=row,
            col=col,
        )

    def _add_enrollees_by_file(
        self, fig: go.Figure, kpis: dict[str, Any], row: int, col: int
    ) -> None:
        """Bar chart of enrollee counts per source file."""
        df = kpis.get("enrollee_count_by_file", pd.DataFrame())
        if df.empty:
            df = pd.DataFrame({"source_file": ["N/A"], "count": [0]})
        fig.add_trace(
            go.Bar(
                x=df["source_file"],
                y=df["count"],
                marker_color="#5B9BD5",
                name="Enrollees",
            ),
            row=row,
            col=col,
        )

    def _add_enrollees_by_period(
        self, fig: go.Figure, kpis: dict[str, Any], row: int, col: int
    ) -> None:
        """Bar chart of enrollee counts per source period (rollup view)."""
        df = kpis.get("member_count_by_source_period", pd.DataFrame())
        if df.empty:
            df = pd.DataFrame({"source_period": ["N/A"], "count": [0]})
        period_col = "source_period" if "source_period" in df.columns else df.columns[0]
        fig.add_trace(
            go.Bar(
                x=df[period_col],
                y=df["count"],
                marker_color="#2E75B6",
                name="Enrollees by Period",
            ),
            row=row,
            col=col,
        )

    def _add_subscriber_pie(
        self, fig: go.Figure, kpis: dict[str, Any], row: int, col: int
    ) -> None:
        """Pie chart of subscribers (Y) vs dependents (N)."""
        df = kpis.get("member_count_by_subscriber_flag", pd.DataFrame())
        if df.empty:
            df = pd.DataFrame({
                "subscriber_flag": ["Y", "N"],
                "count": [
                    kpis.get("total_subscribers", 0),
                    kpis.get("total_dependents", 0),
                ],
            })
        labels = df["subscriber_flag"].apply(
            lambda x: "Subscriber" if x == "Y" else ("Dependent" if x == "N" else str(x))
        )
        fig.add_trace(
            go.Pie(
                labels=labels,
                values=df["count"],
                marker_colors=["#70AD47", "#FFC000"],
            ),
            row=row,
            col=col,
        )

    def _add_premium_by_rating(
        self, fig: go.Figure, kpis: dict[str, Any], row: int, col: int
    ) -> None:
        """Bar chart of total premium by rating area."""
        df = kpis.get("premium_by_rating_area", pd.DataFrame())
        if df.empty:
            return
        value_col = [c for c in df.columns if c.startswith("total_")][0]
        fig.add_trace(
            go.Bar(
                x=df["rating_area"],
                y=df[value_col],
                marker_color="#ED7D31",
            ),
            row=row,
            col=col,
        )

    def _add_effective_month(
        self, fig: go.Figure, kpis: dict[str, Any], row: int, col: int
    ) -> None:
        """Bar chart of member counts by benefit effective month."""
        df = kpis.get("member_count_by_effective_month", pd.DataFrame())
        if df.empty:
            return
        fig.add_trace(
            go.Bar(
                x=df["effective_month"],
                y=df["count"],
                marker_color="#9E480E",
            ),
            row=row,
            col=col,
        )

    def _add_premium_by_period(
        self, fig: go.Figure, kpis: dict[str, Any], row: int, col: int
    ) -> None:
        """Line/bar trend of total premium by source period (rollup view)."""
        df = kpis.get("premium_by_source_period", pd.DataFrame())
        if df.empty:
            return
        value_col = [c for c in df.columns if c.startswith("total_")][0]
        period_col = "source_period" if "source_period" in df.columns else df.columns[0]
        fig.add_trace(
            go.Bar(
                x=df[period_col],
                y=df[value_col],
                marker_color="#C55A11",
                name="Premium by Period",
            ),
            row=row,
            col=col,
        )

    def _add_validation_summary(
        self, fig: go.Figure, validation_df: pd.DataFrame, row: int, col: int
    ) -> None:
        """Bar chart of validation check outcomes by status."""
        if validation_df.empty:
            counts = pd.DataFrame({"status": ["N/A"], "count": [0]})
        else:
            counts = validation_df["status"].value_counts().reset_index()
            counts.columns = ["status", "count"]
        colors = {"PASS": "#70AD47", "WARN": "#FFC000", "FAIL": "#C00000"}
        bar_colors = [colors.get(s, "#AAAAAA") for s in counts["status"]]
        fig.add_trace(
            go.Bar(
                x=counts["status"],
                y=counts["count"],
                marker_color=bar_colors,
            ),
            row=row,
            col=col,
        )

    def _add_missingness(
        self, fig: go.Figure, missingness_df: pd.DataFrame, row: int, col: int
    ) -> None:
        """Bar chart of top 15 columns by missingness percentage."""
        if missingness_df.empty:
            return
        top = missingness_df.head(15)
        fig.add_trace(
            go.Bar(
                x=top["missing_pct"],
                y=top["column"],
                orientation="h",
                marker_color="#7030A0",
            ),
            row=row,
            col=col,
        )

    def _add_duplicate_indicator(
        self, fig: go.Figure, kpis: dict[str, Any], row: int, col: int
    ) -> None:
        """Indicator cards for duplicate member and policy-member counts."""
        dup_member = kpis.get("duplicate_member_count", 0)
        dup_policy = kpis.get("duplicate_policy_member_count", 0)
        fig.add_trace(
            go.Indicator(
                mode="number",
                value=dup_member,
                title={"text": "Duplicate Members"},
                domain={"x": [0, 0.45], "y": [0, 1]},
            ),
            row=row,
            col=col,
        )
        fig.add_trace(
            go.Indicator(
                mode="number",
                value=dup_policy,
                title={"text": "Duplicate Policy+Member"},
                domain={"x": [0.55, 1], "y": [0, 1]},
            ),
            row=row,
            col=col,
        )
