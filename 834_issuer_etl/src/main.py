"""
834 Issuer ETL — main orchestrator.

Discovers every ``source_data/{issuer}/{year}/{month}/`` partition and runs
the full ETL pipeline independently for each. After all monthly partitions for
an issuer complete, builds issuer-level rollup outputs.

Usage:
    python src/main.py
    python src/main.py --issuer 13535
    python src/main.py --issuer 15105 --year 2026
    python src/main.py --issuer 64357 --year 2026 --month 02
"""

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import DEFAULT_ISSUER_EXAMPLE, SOURCE_DATA_DIR
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
    SourcePartition,
    discover_partitions,
    ensure_partition_asset_dirs,
    ensure_rollup_asset_dirs,
    monthly_output_stem,
    rollup_output_stem,
)
from validate.data_quality_validator import DataQualityValidator
from validate.schema_validator import SchemaValidator

logger = get_logger(__name__)


@dataclass
class PartitionRunResult:
    """Outcome of processing a single issuer/year/month partition."""

    partition: SourcePartition
    status: str  # processed | skipped | failed
    output_path: Path | None = None
    enrollee_count: int = 0
    error: str | None = None


@dataclass
class PipelineSummary:
    """Aggregated run statistics printed at the end of execution."""

    discovered: int = 0
    processed: int = 0
    skipped: int = 0
    failed: int = 0
    results: list[PartitionRunResult] = field(default_factory=list)
    rollup_issuers: list[str] = field(default_factory=list)
    failed_rollups: list[str] = field(default_factory=list)


class IssuerEtlPipeline:
    """
    End-to-end ETL pipeline driven by ``discover_partitions()``.

    Each valid issuer/year/month folder is processed independently. Issuer
    rollups are built after all monthly partitions for that issuer succeed.
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
    ) -> PipelineSummary:
        """
        Execute the pipeline for every matching partition, then issuer rollups.

        Args:
            issuer_id: Optional issuer filter.
            year: Optional year filter (typically used with issuer).
            month: Optional month filter (typically used with issuer + year).

        Returns:
            ``PipelineSummary`` with counts and per-partition outcomes.
        """
        partitions = discover_partitions(
            source_root=SOURCE_DATA_DIR,
            issuer_id=issuer_id,
            year=year,
            month=month,
        )

        summary = PipelineSummary(discovered=len(partitions))

        if not partitions:
            logger.error(
                "No partitions found under %s. Expected structure: "
                "source_data/{issuer_id}/{year}/{month}/*.xml",
                SOURCE_DATA_DIR,
            )
            self._print_summary(summary)
            sys.exit(1)

        issuer_monthly_dfs: dict[str, list[pd.DataFrame]] = {}

        for partition in partitions:
            logger.info(
                "Processing issuer=%s year=%s month=%s files=%d path=%s",
                partition.issuer_id,
                partition.year,
                partition.month,
                partition.file_count,
                partition.input_path,
            )
            try:
                cleaned_df, run_result = self._process_partition(partition)
                summary.results.append(run_result)

                if run_result.status == "processed":
                    summary.processed += 1
                    issuer_monthly_dfs.setdefault(partition.issuer_id, []).append(
                        cleaned_df
                    )
                elif run_result.status == "skipped":
                    summary.skipped += 1
                else:
                    summary.failed += 1

            except Exception as exc:
                summary.failed += 1
                error_msg = str(exc)
                logger.error(
                    "Partition %s failed: %s",
                    partition.period_key,
                    error_msg,
                    exc_info=True,
                )
                summary.results.append(
                    PartitionRunResult(
                        partition=partition,
                        status="failed",
                        error=error_msg,
                    )
                )

        for iid, dfs in issuer_monthly_dfs.items():
            if not dfs:
                continue
            try:
                self._process_rollup(iid, dfs)
                summary.rollup_issuers.append(iid)
            except Exception as exc:
                summary.failed_rollups.append(iid)
                logger.error(
                    "Rollup for issuer %s failed: %s", iid, exc, exc_info=True
                )

        self._print_summary(summary)
        return summary

    def _process_partition(
        self, partition: SourcePartition
    ) -> tuple[pd.DataFrame | None, PartitionRunResult]:
        """
        Run the full ETL for one issuer/year/month partition.

        Args:
            partition: Discovered partition with XML files and output path.

        Returns:
            Tuple of (cleaned DataFrame or None, run result metadata).
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
            "Partition %s: %d file(s) parsed, %d file(s) failed",
            partition.period_key,
            files_processed,
            files_failed,
        )

        if not all_rows:
            logger.warning(
                "Skipping partition %s — no enrollee rows parsed",
                partition.period_key,
            )
            return None, PartitionRunResult(
                partition=partition,
                status="skipped",
                output_path=partition.output_path,
            )

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

        logger.info(
            "Completed partition %s — %d enrollees → %s",
            partition.period_key,
            len(cleaned_df),
            partition.output_path,
        )

        return cleaned_df, PartitionRunResult(
            partition=partition,
            status="processed",
            output_path=partition.output_path,
            enrollee_count=len(cleaned_df),
        )

    def _process_rollup(
        self, issuer_id: str, monthly_dfs: list[pd.DataFrame]
    ) -> None:
        """
        Combine monthly DataFrames and generate issuer-level rollup outputs.

        Args:
            issuer_id: Issuer identifier.
            monthly_dfs: Cleaned monthly DataFrames for this issuer.
        """
        logger.info(
            "Building rollup for issuer=%s across %d month(s)",
            issuer_id,
            len(monthly_dfs),
        )

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
        self.excel_exporter.export_validation_report(
            validation_df,
            missingness_df,
            file_profile_df,
            output_stem,
            asset_dirs["excel"],
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

        json_path = (
            asset_dirs["validation_reports"]
            / f"validation_report_{output_stem}.json"
        )
        self._write_validation_json(validation_df, json_path)

        logger.info(
            "Rollup complete for issuer=%s → %s", issuer_id, asset_dirs["base"]
        )

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
        partition: SourcePartition,
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
        records = (
            validation_df.to_dict(orient="records")
            if not validation_df.empty
            else []
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(records, indent=2), encoding="utf-8")
        logger.info("Exported validation JSON: %s", path)

    @staticmethod
    def _print_summary(summary: PipelineSummary) -> None:
        """Print end-of-run processing summary to the log."""
        logger.info("=" * 60)
        logger.info("PROCESSING SUMMARY")
        logger.info("=" * 60)
        logger.info("Total partitions discovered : %d", summary.discovered)
        logger.info("Total partitions processed  : %d", summary.processed)
        logger.info("Total partitions skipped    : %d", summary.skipped)
        logger.info("Total partitions failed     : %d", summary.failed)

        if summary.rollup_issuers:
            logger.info(
                "Rollups created for issuers : %s",
                ", ".join(summary.rollup_issuers),
            )
        if summary.failed_rollups:
            logger.info(
                "Rollups failed for issuers  : %s",
                ", ".join(summary.failed_rollups),
            )

        if summary.results:
            logger.info("-" * 60)
            logger.info("Partition outputs:")
            for result in summary.results:
                p = result.partition
                if result.status == "processed":
                    logger.info(
                        "  [OK]   issuer=%s year=%s month=%s "
                        "enrollees=%d → %s",
                        p.issuer_id,
                        p.year,
                        p.month,
                        result.enrollee_count,
                        result.output_path,
                    )
                elif result.status == "skipped":
                    logger.info(
                        "  [SKIP] issuer=%s year=%s month=%s — no data",
                        p.issuer_id,
                        p.year,
                        p.month,
                    )
                else:
                    logger.info(
                        "  [FAIL] issuer=%s year=%s month=%s — %s",
                        p.issuer_id,
                        p.year,
                        p.month,
                        result.error or "unknown error",
                    )

        logger.info("=" * 60)


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
    summary = pipeline.run(
        issuer_id=args.issuer,
        year=args.year,
        month=args.month,
    )
    if summary.failed > 0 or summary.failed_rollups:
        sys.exit(1)


if __name__ == "__main__":
    main()
