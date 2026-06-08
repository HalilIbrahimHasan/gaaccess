"""
SQLite loader — persists enrollees, KPIs, and validation results.

Creates per-partition and rollup databases under ``assets/`` with table names
defined in config for ad-hoc SQL analysis.
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from config import (
    TABLE_ENROLLEES,
    TABLE_ENROLLEES_ROLLUP,
    TABLE_KPIS,
    TABLE_KPIS_ROLLUP,
    TABLE_VALIDATION,
    TABLE_VALIDATION_ROLLUP,
)
from utils.logger import get_logger

logger = get_logger(__name__)


class SqliteLoader:
    """
    Load cleaned enrollee data, KPIs, and validation results into SQLite.

    Supports monthly partition databases and issuer rollup databases with
    distinct table names for each scope.
    """

    def load(
        self,
        df: pd.DataFrame,
        kpi_summary_df: pd.DataFrame,
        validation_df: pd.DataFrame,
        output_stem: str,
        output_dir: Path,
        *,
        rollup: bool = False,
    ) -> Path:
        """
        Create or replace SQLite tables for a partition or rollup.

        Args:
            df: Cleaned enrollee DataFrame.
            kpi_summary_df: Scalar KPI summary.
            validation_df: Validation check results.
            output_stem: Filename stem (e.g. ``64357_2026_02``).
            output_dir: Target sqlite directory.
            rollup: Use rollup table names when True.

        Returns:
            Path to ``issuer_{output_stem}.db``.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        db_path = output_dir / f"issuer_{output_stem}.db"

        enrollee_table = TABLE_ENROLLEES_ROLLUP if rollup else TABLE_ENROLLEES
        kpi_table = TABLE_KPIS_ROLLUP if rollup else TABLE_KPIS
        validation_table = (
            TABLE_VALIDATION_ROLLUP if rollup else TABLE_VALIDATION
        )

        with sqlite3.connect(db_path) as conn:
            self._load_enrollees(df, conn, enrollee_table)
            self._load_kpis(kpi_summary_df, output_stem, conn, kpi_table)
            self._load_validation(validation_df, conn, validation_table)

        logger.info("Loaded SQLite database: %s", db_path)
        return db_path

    def _load_enrollees(
        self, df: pd.DataFrame, conn: sqlite3.Connection, table_name: str
    ) -> None:
        """Replace the enrollee table with current partition or rollup data."""
        export_df = df.copy()
        if not export_df.empty:
            export_df.insert(0, "id", range(1, len(export_df) + 1))
        export_df.to_sql(table_name, conn, if_exists="replace", index=False)
        logger.debug("Loaded %d rows into %s", len(export_df), table_name)

    def _load_kpis(
        self,
        kpi_summary_df: pd.DataFrame,
        output_stem: str,
        conn: sqlite3.Connection,
        table_name: str,
    ) -> None:
        """Replace KPI table with the latest scalar metrics for this scope."""
        kpi_df = kpi_summary_df.copy()
        kpi_df["output_stem"] = output_stem
        kpi_df["load_timestamp"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        kpi_df.to_sql(table_name, conn, if_exists="replace", index=False)
        logger.debug("Loaded KPIs into %s", table_name)

    def _load_validation(
        self,
        validation_df: pd.DataFrame,
        conn: sqlite3.Connection,
        table_name: str,
    ) -> None:
        """Replace the validation table with latest check outcomes."""
        columns = [
            "issuer_id", "source_year", "source_month", "source_period",
            "check_category", "check_name", "status",
            "message", "details", "affected_count",
        ]
        if validation_df.empty:
            pd.DataFrame(columns=columns).to_sql(
                table_name, conn, if_exists="replace", index=False
            )
        else:
            validation_df.to_sql(table_name, conn, if_exists="replace", index=False)
        logger.debug("Loaded %d validation result(s) into %s", len(validation_df), table_name)

    @staticmethod
    def kpis_to_json(kpis: dict[str, Any]) -> str:
        """Serialize KPI dict to JSON, excluding non-serializable DataFrames."""
        serializable = {
            k: v for k, v in kpis.items() if not isinstance(v, pd.DataFrame)
        }
        return json.dumps(serializable, default=str)
