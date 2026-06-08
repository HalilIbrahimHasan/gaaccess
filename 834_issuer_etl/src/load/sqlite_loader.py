"""
SQLite loader — persists enrollees, KPIs, and validation results.

Creates per-issuer databases under ``assets/{issuer_id}/sqlite/`` with
tables defined in config for ad-hoc SQL analysis.
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from config import TABLE_ENROLLEES, TABLE_KPIS, TABLE_VALIDATION
from utils.logger import get_logger

logger = get_logger(__name__)


class SqliteLoader:
    """
    Load cleaned enrollee data, KPIs, and validation results into SQLite.

    Uses ``sqlite3`` directly for zero extra dependencies; each issuer gets
    an isolated ``issuer_{issuer_id}.db`` file.
    """

    def load(
        self,
        df: pd.DataFrame,
        kpi_summary_df: pd.DataFrame,
        validation_df: pd.DataFrame,
        issuer_id: str,
        output_dir: Path,
    ) -> Path:
        """
        Create or replace SQLite tables for an issuer.

        Args:
            df: Cleaned enrollee DataFrame.
            kpi_summary_df: Scalar KPI summary.
            validation_df: Validation check results.
            issuer_id: Issuer identifier.
            output_dir: Target ``assets/{issuer_id}/sqlite`` directory.

        Returns:
            Path to ``issuer_{issuer_id}.db``.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        db_path = output_dir / f"issuer_{issuer_id}.db"

        with sqlite3.connect(db_path) as conn:
            self._load_enrollees(df, conn)
            self._load_kpis(kpi_summary_df, issuer_id, conn)
            self._load_validation(validation_df, conn)

        logger.info("Loaded SQLite database: %s", db_path)
        return db_path

    def _load_enrollees(self, df: pd.DataFrame, conn: sqlite3.Connection) -> None:
        """
        Replace the ``issuer_enrollees`` table with current enrollee data.

        Adds a surrogate ``id`` primary key for relational queries.
        """
        export_df = df.copy()
        export_df.insert(0, "id", range(1, len(export_df) + 1))
        export_df.to_sql(TABLE_ENROLLEES, conn, if_exists="replace", index=False)
        logger.debug("Loaded %d rows into %s", len(export_df), TABLE_ENROLLEES)

    def _load_kpis(
        self, kpi_summary_df: pd.DataFrame, issuer_id: str, conn: sqlite3.Connection
    ) -> None:
        """
        Append scalar KPIs to ``issuer_kpis`` with a load timestamp.

        Uses append mode so historical KPI runs can be retained if desired;
        callers may truncate manually for a single-snapshot view.
        """
        kpi_df = kpi_summary_df.copy()
        kpi_df["issuer_id"] = issuer_id
        kpi_df["load_timestamp"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # Create table on first run, then append
        existing = pd.read_sql(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_KPIS}'",
            conn,
        )
        if_exists = "append" if not existing.empty else "replace"
        kpi_df.to_sql(TABLE_KPIS, conn, if_exists=if_exists, index=False)
        logger.debug("Loaded KPIs into %s (mode=%s)", TABLE_KPIS, if_exists)

    def _load_validation(
        self, validation_df: pd.DataFrame, conn: sqlite3.Connection
    ) -> None:
        """Replace the ``validation_results`` table with latest check outcomes."""
        if validation_df.empty:
            pd.DataFrame(columns=[
                "issuer_id", "check_category", "check_name", "status",
                "message", "details", "affected_count",
            ]).to_sql(TABLE_VALIDATION, conn, if_exists="replace", index=False)
        else:
            validation_df.to_sql(
                TABLE_VALIDATION, conn, if_exists="replace", index=False
            )
        logger.debug("Loaded %d validation result(s)", len(validation_df))

    @staticmethod
    def kpis_to_json(kpis: dict[str, Any]) -> str:
        """
        Serialize KPI dict to JSON, excluding non-serializable DataFrames.

        Useful for logging or API responses in future extensions.
        """
        serializable = {
            k: v
            for k, v in kpis.items()
            if not isinstance(v, pd.DataFrame)
        }
        return json.dumps(serializable, default=str)
