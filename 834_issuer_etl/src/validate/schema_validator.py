"""
Schema validation — verifies required columns exist in cleaned enrollee data.

Runs before data-quality rules so downstream checks can assume a stable
column contract defined in ``config.REQUIRED_COLUMNS``.
"""

import pandas as pd

from config import REQUIRED_COLUMNS
from utils.logger import get_logger
from utils.partition import Partition

logger = get_logger(__name__)


class SchemaValidator:
    """
    Validate that a cleaned DataFrame conforms to the expected column schema.

    Produces structured pass/fail results consumed by validation reports,
    SQLite persistence, and the dashboard issue summary.
    """

    def validate(
        self,
        df: pd.DataFrame,
        issuer_id: str,
        partition: Partition | None = None,
    ) -> list[dict]:
        """
        Check presence of all required columns.

        Args:
            df: Cleaned enrollee DataFrame.
            issuer_id: Issuer being validated (for report context).
            partition: Optional issuer/year/month partition for result labeling.

        Returns:
            List of validation result dicts with ``check``, ``status``,
            ``message``, and ``details`` keys.
        """
        results: list[dict] = []
        missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        ctx = _partition_context(issuer_id, partition)

        if missing:
            results.append({
                **ctx,
                "check_category": "schema",
                "check_name": "required_columns_exist",
                "status": "FAIL",
                "message": f"Missing {len(missing)} required column(s)",
                "details": ", ".join(missing),
                "affected_count": len(missing),
            })
            logger.error(
                "Schema validation FAILED for %s — missing: %s",
                _partition_label(partition, issuer_id),
                missing,
            )
        else:
            results.append({
                **ctx,
                "check_category": "schema",
                "check_name": "required_columns_exist",
                "status": "PASS",
                "message": f"All {len(REQUIRED_COLUMNS)} required columns present",
                "details": "",
                "affected_count": 0,
            })
            logger.info(
                "Schema validation PASSED for %s",
                _partition_label(partition, issuer_id),
            )

        return results


def _partition_context(
    issuer_id: str, partition: Partition | None
) -> dict[str, str]:
    """Build partition metadata fields for validation result records."""
    if partition is None:
        return {
            "issuer_id": issuer_id,
            "source_year": "",
            "source_month": "",
            "source_period": "all_periods",
        }
    return {
        "issuer_id": partition.issuer_id,
        "source_year": partition.year,
        "source_month": partition.month,
        "source_period": partition.source_period,
    }


def _partition_label(partition: Partition | None, issuer_id: str) -> str:
    """Return a log-friendly partition label."""
    if partition is None:
        return f"issuer {issuer_id} (rollup)"
    return f"{partition.period_key}"
