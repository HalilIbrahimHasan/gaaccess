"""
834 Issuer ETL — main orchestrator.

Runs the full extract → transform → validate → load → dashboard pipeline
for all issuer/year/month partitions discovered under ``source_data/``.

Usage:
    python src/main.py
    python src/main.py --issuer 64357
    python src/main.py --issuer 64357 --year 2026
    python src/main.py --issuer 64357 --year 2026 --month 02
"""

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import DEFAULT_ISSUER_EXAMPLE
from dashboard.plotly_dashboard import PlotlyDashboard
from extract.xml_reader import LocalFileSource, XmlReader
from load.excel_exporter import ExcelExporter
from load.sqlite_loader import SqliteLoader
from load.xml_exporter import XmlExporter
from transform.cleaner import DataCleaner
from transform.kpi_builder import KpiBuilder
from transform.xml_parser import Xml834Parser
from utils.logger import get_logger
from utils.partition import (
    Partition,
    ensure_partition_asset_dirs,
    ensure_rollup_asset_dirs,
    monthly_output_stem,
    rollup_output_stem,
)
from validate.data_quality_validator import DataQualityValidator
from validate.schema_validator import SchemaValidator

logger = get_logger(__name__)


class IssuerEtlPipeline:
    """
    End-to-end ETL pipeline for partitioned 834 XML issuer enrollment data.

    Processes each issuer/year/month partition independently, then builds
    optional issuer-level rollup outputs across all processed months.
    """

    def __init__(self) -> None:
        """Initialize pipeline components (stateless — safe to reuse)."""
        self.reader = XmlReader(source=LocalFileSource())
        self.parser = Xml834Parser()
        self.cleaner = DataCleaner()
        self.kpi_builder = KpiBuilder()
        self.schema_validator = SchemaValidator()
        self.dq_validator = DataQualityValidator()
        self.excel_exporter = ExcelExporter()
        self.xml_exporter = XmlExporter()
        self.sqlite_loader = SqliteLoader()
        self.dashboard = PlotlyDashboard()

    def run(
        self,
        issuer_id: str | None = None,
        year: str | None = None,
        month: str | None = None,
    ) -> None:
        """
        Execute the pipeline for matching partitions and issuer rollups.

        Args:
            issuer_id: Optional issuer filter.
            year: Optional year filter (typically used with issuer).
            month: Optional month filter (typically used with issuer + year).
        """
        partitions = self.reader.discover_partitions(
            issuer_id=issuer_id, year=year, month=month
        )

        if not partitions:
            logger.error(
                "No partitions found. Expected structure: "
                "source_data/{issuer_id}/{year}/{month}/*.xml"
            )
            sys.exit(1)

        issuer_monthly_dfs: dict[str, list[pd.DataFrame]] = {}

        for partition in partitions:
            logger.info("=" * 60)
            logger.info(
                "Processing partition: %s / %s / %s",
                partition.issuer_id,
                partition.year,
                partition.month,
            )
            logger.info("=" * 60)
            try:
                cleaned_df = self._process_partition(partition)
                if cleaned_df is not None and not cleaned_df.empty:
                    issuer_monthly_dfs.setdefault(partition.issuer_id, []).append(
                        cleaned_df
                    )
            except Exception as exc:
                logger.error(
                    "Partition %s failed: %s", partition.period_key, exc,
                    exc_info=True,
                )

        for iid, dfs in issuer_monthly_dfs.items():
            if len(dfs) >= 1:
                try:
                    self._process_rollup(iid, dfs)
                except Exception as exc:
                    logger.error(
                        "Rollup for issuer %s failed: %s", iid, exc, exc_info=True
                    )

    def _process_partition(self, partition: Partition) -> pd.DataFrame | None:
        """
        Run extract through dashboard for a single issuer/year/month partition.

        Args:
            partition: Discovered partition with input XML files and output path.

        Returns:
            Cleaned enrollee DataFrame, or ``None`` when no data was parsed.
        """
        asset_dirs = ensure_partition_asset_dirs(partition)
        output_stem = monthly_output_stem(partition)
        records = self.reader.get_file_records(partition)

        all_rows: list[dict] = []
        files_processed = 0
        files_failed = 0

        for record in records:
            try:
                xml_bytes = self.reader.read_xml_content(record)
                rows = self.parser.parse(
                    xml_bytes,
                    source_file=record.source_file,
                    issuer_id=record.issuer_id,
                )
                all_rows.extend(rows)
                files_processed += 1
            except Exception as exc:
                files_failed += 1
                logger.error(
                    "Failed to parse %s: %s — continuing",
                    record.source_file,
                    exc,
                )

        logger.info(
            "Partition %s: %d file(s) OK, %d file(s) failed",
            partition.period_key,
            files_processed,
            files_failed,
        )

        if not all_rows:
            logger.warning("No enrollee rows for partition %s", partition.period_key)
            return None

        raw_df = pd.DataFrame(all_rows)
        cleaned_df = self.cleaner.clean(raw_df, partition=partition)

        schema_results = self.schema_validator.validate(
            cleaned_df, partition.issuer_id, partition=partition
        )
        dq_results = self.dq_validator.validate(
            cleaned_df, partition.issuer_id, partition=partition
        )
        validation_results = schema_results + dq_results
        validation_df = self.dq_validator.results_to_dataframe(validation_results)
        missingness_df = self.dq_validator.build_missingness_df(cleaned_df)
        file_profile_df = self.dq_validator.build_file_profile_df(cleaned_df)

        kpis = self.kpi_builder.build_kpis(cleaned_df, partition.issuer_id)
        kpi_summary_df = self.kpi_builder.kpis_to_summary_df(kpis)

        self._export_partition_outputs(
            cleaned_df=cleaned_df,
            kpis=kpis,
            kpi_summary_df=kpi_summary_df,
            validation_df=validation_df,
            missingness_df=missingness_df,
            file_profile_df=file_profile_df,
            output_stem=output_stem,
            asset_dirs=asset_dirs,
            partition=partition,
        )

        logger.info("Partition %s complete.", partition.period_key)
        return cleaned_df

    def _process_rollup(
        self, issuer_id: str, monthly_dfs: list[pd.DataFrame]
    ) -> None:
        """
        Combine monthly DataFrames and generate issuer-level rollup outputs.

        Args:
            issuer_id: Issuer identifier.
            monthly_dfs: List of cleaned monthly DataFrames for this issuer.
        """
        logger.info("=" * 60)
        logger.info("Building rollup for issuer: %s (%d month(s))", issuer_id, len(monthly_dfs))
        logger.info("=" * 60)

        combined_df = pd.concat(monthly_dfs, ignore_index=True)
        asset_dirs = ensure_rollup_asset_dirs(issuer_id)
        output_stem = rollup_output_stem(issuer_id)

        schema_results = self.schema_validator.validate(
            combined_df, issuer_id, partition=None
        )
        dq_results = self.dq_validator.validate(
            combined_df, issuer_id, partition=None
        )
        validation_results = schema_results + dq_results
        validation_df = self.dq_validator.results_to_dataframe(validation_results)
        missingness_df = self.dq_validator.build_missingness_df(combined_df)
        file_profile_df = self.dq_validator.build_file_profile_df(combined_df)

        kpis = self.kpi_builder.build_kpis(combined_df, issuer_id)
        kpi_summary_df = self.kpi_builder.kpis_to_summary_df(kpis)

        self.excel_exporter.export_enrollees(
            combined_df, output_stem, asset_dirs["excel"]
        )
        self.excel_exporter.export_kpis(
            kpis, kpi_summary_df, output_stem, asset_dirs["excel"], is_rollup=True
        )
        self.sqlite_loader.load(
            combined_df,
            kpi_summary_df,
            validation_df,
            output_stem,
            asset_dirs["sqlite"],
            rollup=True,
        )
        self.dashboard.generate(
            kpis,
            kpi_summary_df,
            validation_df,
            missingness_df,
            output_stem,
            asset_dirs["dashboards"],
            title=f"Issuer {issuer_id} — All Periods Rollup Dashboard",
            is_rollup=True,
        )

        logger.info("Rollup for issuer %s complete.", issuer_id)
        logger.info("  Excel:     %s", asset_dirs["excel"])
        logger.info("  SQLite:    %s", asset_dirs["sqlite"])
        logger.info("  Dashboard: %s", asset_dirs["dashboards"])

    def _export_partition_outputs(
        self,
        *,
        cleaned_df: pd.DataFrame,
        kpis: dict,
        kpi_summary_df: pd.DataFrame,
        validation_df: pd.DataFrame,
        missingness_df: pd.DataFrame,
        file_profile_df: pd.DataFrame,
        output_stem: str,
        asset_dirs: dict[str, Path],
        partition: Partition,
    ) -> None:
        """Write all monthly export artifacts for one partition."""
        self.excel_exporter.export_enrollees(
            cleaned_df, output_stem, asset_dirs["excel"]
        )
        self.excel_exporter.export_kpis(
            kpis, kpi_summary_df, output_stem, asset_dirs["excel"]
        )
        self.excel_exporter.export_validation_report(
            validation_df,
            missingness_df,
            file_profile_df,
            output_stem,
            asset_dirs["excel"],
        )
        self.xml_exporter.export_enrollees(
            cleaned_df, output_stem, asset_dirs["cleaned_xml"]
        )
        self.sqlite_loader.load(
            cleaned_df,
            kpi_summary_df,
            validation_df,
            output_stem,
            asset_dirs["sqlite"],
            rollup=False,
        )
        self.dashboard.generate(
            kpis,
            kpi_summary_df,
            validation_df,
            missingness_df,
            output_stem,
            asset_dirs["dashboards"],
            title=(
                f"Issuer {partition.issuer_id} — "
                f"{partition.year}/{partition.month} Dashboard"
            ),
            is_rollup=False,
        )

        json_path = (
            asset_dirs["validation_reports"]
            / f"validation_report_{output_stem}.json"
        )
        self._write_validation_json(validation_df, json_path)

    @staticmethod
    def _write_validation_json(
        validation_df: pd.DataFrame, path: Path
    ) -> None:
        """Persist validation results as JSON for programmatic consumption."""
        records = validation_df.to_dict(orient="records") if not validation_df.empty else []
        path.write_text(json.dumps(records, indent=2), encoding="utf-8")
        logger.info("Exported validation JSON: %s", path)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for issuer/year/month partition filters."""
    parser = argparse.ArgumentParser(
        description="834 XML Issuer ETL Framework (partitioned)",
    )
    parser.add_argument(
        "--issuer",
        type=str,
        default=None,
        help=f"Process one issuer (e.g. {DEFAULT_ISSUER_EXAMPLE})",
    )
    parser.add_argument(
        "--year",
        type=str,
        default=None,
        help="Process one year (e.g. 2026); use with --issuer",
    )
    parser.add_argument(
        "--month",
        type=str,
        default=None,
        help="Process one month (e.g. 02); use with --issuer and --year",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entry point."""
    args = parse_args()
    pipeline = IssuerEtlPipeline()
    pipeline.run(issuer_id=args.issuer, year=args.year, month=args.month)


if __name__ == "__main__":
    main()
