"""
834 Issuer ETL — main orchestrator.

Runs the full extract → transform → validate → load → dashboard pipeline
for all issuers discovered under ``source_data/``, or a single issuer when
``--issuer`` is specified.

Usage:
    python src/main.py
    python src/main.py --issuer 64357
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

# Ensure src/ is on the path when running as a script
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
from utils.file_utils import ensure_issuer_asset_dirs
from utils.logger import get_logger
from validate.data_quality_validator import DataQualityValidator
from validate.schema_validator import SchemaValidator

logger = get_logger(__name__)


class IssuerEtlPipeline:
    """
    End-to-end ETL pipeline for a single or multiple 834 XML issuers.

    Coordinates all stages while isolating failures at the file level so one
    malformed XML does not halt processing of remaining files.
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

    def run(self, issuer_id: str | None = None) -> None:
        """
        Execute the pipeline for one or all issuers.

        Args:
            issuer_id: Optional single issuer to process; ``None`` runs all.
        """
        issuer_ids = (
            [issuer_id]
            if issuer_id
            else self.reader.discover_issuers()
        )

        if not issuer_ids:
            logger.error(
                "No issuer folders found under source_data/. "
                "Add folders like source_data/64357/ with XML files."
            )
            sys.exit(1)

        for iid in issuer_ids:
            logger.info("=" * 60)
            logger.info("Processing issuer: %s", iid)
            logger.info("=" * 60)
            try:
                self._process_issuer(iid)
            except Exception as exc:
                logger.error(
                    "Issuer %s failed with unexpected error: %s", iid, exc,
                    exc_info=True,
                )

    def _process_issuer(self, issuer_id: str) -> None:
        """
        Run extract through dashboard for a single issuer.

        Args:
            issuer_id: Issuer folder name (e.g. ``64357``).
        """
        asset_dirs = ensure_issuer_asset_dirs(issuer_id)
        records = self.reader.get_file_records(issuer_id)

        if not records:
            logger.warning("No XML files found for issuer %s — skipping", issuer_id)
            return

        # --- Extract & Transform ---
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
                    "Failed to parse %s: %s — continuing with remaining files",
                    record.source_file,
                    exc,
                )

        logger.info(
            "Issuer %s: %d file(s) OK, %d file(s) failed",
            issuer_id,
            files_processed,
            files_failed,
        )

        raw_df = pd.DataFrame(all_rows)
        cleaned_df = self.cleaner.clean(raw_df)

        # --- Validate ---
        schema_results = self.schema_validator.validate(cleaned_df, issuer_id)
        dq_results = self.dq_validator.validate(cleaned_df, issuer_id)
        validation_results = schema_results + dq_results
        validation_df = self.dq_validator.results_to_dataframe(validation_results)
        missingness_df = self.dq_validator.build_missingness_df(cleaned_df)
        file_profile_df = self.dq_validator.build_file_profile_df(cleaned_df)

        # --- KPIs ---
        kpis = self.kpi_builder.build_kpis(cleaned_df, issuer_id)
        kpi_summary_df = self.kpi_builder.kpis_to_summary_df(kpis)

        # --- Load / Export ---
        self.excel_exporter.export_enrollees(
            cleaned_df, issuer_id, asset_dirs["excel"]
        )
        self.excel_exporter.export_kpis(
            kpis, kpi_summary_df, issuer_id, asset_dirs["excel"]
        )
        self.excel_exporter.export_validation_report(
            validation_df,
            missingness_df,
            file_profile_df,
            issuer_id,
            asset_dirs["excel"],
        )
        self.xml_exporter.export_enrollees(
            cleaned_df, issuer_id, asset_dirs["cleaned_xml"]
        )
        self.sqlite_loader.load(
            cleaned_df,
            kpi_summary_df,
            validation_df,
            issuer_id,
            asset_dirs["sqlite"],
        )
        self.dashboard.generate(
            kpis,
            kpi_summary_df,
            validation_df,
            missingness_df,
            issuer_id,
            asset_dirs["dashboards"],
        )

        # Persist validation report copy
        validation_path = (
            asset_dirs["validation_reports"] / f"validation_report_{issuer_id}.csv"
        )
        validation_df.to_csv(validation_path, index=False)

        logger.info("Issuer %s processing complete.", issuer_id)
        logger.info("  Excel:      %s", asset_dirs["excel"])
        logger.info("  XML:        %s", asset_dirs["cleaned_xml"])
        logger.info("  SQLite:     %s", asset_dirs["sqlite"])
        logger.info("  Dashboard:  %s", asset_dirs["dashboards"])


def parse_args() -> argparse.Namespace:
    """
    Parse CLI arguments for optional single-issuer processing.

    Returns:
        Parsed namespace with optional ``issuer`` attribute.
    """
    parser = argparse.ArgumentParser(
        description="834 XML Issuer ETL Framework",
    )
    parser.add_argument(
        "--issuer",
        type=str,
        default=None,
        help=f"Process a single issuer only (e.g. {DEFAULT_ISSUER_EXAMPLE})",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entry point."""
    args = parse_args()
    pipeline = IssuerEtlPipeline()
    pipeline.run(issuer_id=args.issuer)


if __name__ == "__main__":
    main()
