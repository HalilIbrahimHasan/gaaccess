"""
Excel exporter — writes cleaned enrollees, KPIs, and validation reports.

Produces multi-sheet workbooks per partition or rollup under the configured
``assets/`` hierarchy for analyst review and downstream reporting.
"""

from pathlib import Path
from typing import Any

import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)


class ExcelExporter:
    """
    Export enrollee datasets to formatted Excel workbooks.

    Uses an ``output_stem`` label (e.g. ``64357_2026_02`` or
    ``64357_all_periods``) so monthly and rollup files follow one naming
    convention.
    """

    def export_enrollees(
        self, df: pd.DataFrame, output_stem: str, output_dir: Path
    ) -> Path:
        """
        Write cleaned enrollee records to Excel.

        Args:
            df: Cleaned enrollee DataFrame.
            output_stem: Filename stem for this partition or rollup.
            output_dir: Target excel directory.

        Returns:
            Path to the written ``.xlsx`` file.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / f"cleaned_enrollees_{output_stem}.xlsx"
        df.to_excel(path, index=False, sheet_name="enrollees")
        logger.info("Exported enrollees to %s (%d rows)", path, len(df))
        return path

    def export_kpis(
        self,
        kpis: dict[str, Any],
        kpi_summary_df: pd.DataFrame,
        output_stem: str,
        output_dir: Path,
        *,
        is_rollup: bool = False,
    ) -> Path:
        """
        Write KPI summary and dimensional breakdowns to a multi-sheet workbook.

        Args:
            kpis: Full KPI dict from ``KpiBuilder``.
            kpi_summary_df: Scalar KPI summary DataFrame.
            output_stem: Filename stem for this partition or rollup.
            output_dir: Target excel directory.
            is_rollup: When True, include cross-period breakdown sheets.

        Returns:
            Path to the KPI workbook.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / f"kpi_summary_{output_stem}.xlsx"

        breakdown_sheets = {
            "subscriber_flag": "member_count_by_subscriber_flag",
            "relationship_code": "member_count_by_relationship_code",
            "event_type": "member_count_by_event_type",
            "event_reason": "member_count_by_event_reason",
            "maintenance_type": "member_count_by_maintenance_type",
            "insurance_type": "member_count_by_insurance_type",
            "rating_area": "member_count_by_rating_area",
            "effective_month": "member_count_by_effective_month",
            "premium_rating_area": "premium_by_rating_area",
            "premium_effective_month": "premium_by_effective_month",
            "file_trend": "file_count_trend",
            "enrollee_by_file": "enrollee_count_by_file",
        }
        if is_rollup:
            breakdown_sheets["source_period"] = "member_count_by_source_period"
            breakdown_sheets["premium_period"] = "premium_by_source_period"

        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            kpi_summary_df.to_excel(writer, sheet_name="summary", index=False)
            for sheet_name, kpi_key in breakdown_sheets.items():
                breakdown_df = kpis.get(kpi_key, pd.DataFrame())
                if isinstance(breakdown_df, pd.DataFrame) and not breakdown_df.empty:
                    breakdown_df.to_excel(
                        writer, sheet_name=sheet_name[:31], index=False
                    )

        logger.info("Exported KPI workbook to %s", path)
        return path

    def export_validation_report(
        self,
        validation_df: pd.DataFrame,
        missingness_df: pd.DataFrame,
        file_profile_df: pd.DataFrame,
        output_stem: str,
        output_dir: Path,
    ) -> Path:
        """
        Write validation results, missingness, and file profiles to Excel.

        Args:
            validation_df: All validation check results.
            missingness_df: Column missingness percentages.
            file_profile_df: Per-file row/policy/member counts.
            output_stem: Filename stem for this partition or rollup.
            output_dir: Target excel directory.

        Returns:
            Path to the validation workbook.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / f"validation_report_{output_stem}.xlsx"

        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            validation_df.to_excel(writer, sheet_name="validation_checks", index=False)
            missingness_df.to_excel(writer, sheet_name="missingness", index=False)
            file_profile_df.to_excel(writer, sheet_name="file_profile", index=False)

        logger.info("Exported validation report to %s", path)
        return path
