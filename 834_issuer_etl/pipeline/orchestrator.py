"""
Full pipeline orchestrator: ingest → parse → load → validate → reconcile → report.
"""

from __future__ import annotations

from config.config import settings
from connectors.base_connector import SourceConnector
from connectors.ftp_connector import FTPSourceConnector
from connectors.local_connector import LocalSourceConnector
from connectors.sftp_connector import SFTPSourceConnector
from database.db import Database
from database.loaders import DataLoader
from ingestion.xml_reader import read_xml_bytes
from parsers.parser_834 import Parser834
from reconciliation.cancellation_window import apply_cancellation_window
from reconciliation.premium_validation import apply_premium_validation
from reconciliation.user_fee_calculation import apply_user_fees
from pipeline.assets_exporter import export_assets
from reporting.report_runner import run_kpi_reports
from utils.cleanup import clean_workspace
from utils.logger import get_logger
from validation.load_validation import run_load_validation

logger = get_logger(__name__)


def get_connector() -> SourceConnector:
    mode = settings.processing_mode
    if mode == "ftp":
        return FTPSourceConnector()
    if mode == "sftp":
        return SFTPSourceConnector()
    return LocalSourceConnector()


class Pipeline:
    def __init__(self) -> None:
        if settings.clean_on_start:
            clean_workspace(clear_source=False)

        settings.ensure_dirs()
        self.db = Database()
        self.db.init_schema()
        self.loader = DataLoader(self.db)
        self.parser = Parser834()

    def ingest_and_load(self) -> dict:
        connector = get_connector()
        logger.info("Processing mode: %s", connector.mode())
        sources = connector.sync()
        stats = {
            "files_discovered": len(sources),
            "files_processed": 0,
            "files_skipped": 0,
            "files_failed": 0,
            "records_loaded": 0,
        }

        for source in sources:
            try:
                file_id, is_dup = self.loader.register_file(source)
                if is_dup:
                    stats["files_skipped"] += 1
                    continue

                xml_bytes = read_xml_bytes(source)
                records = self.parser.parse_file(
                    xml_bytes,
                    issuer=source.issuer,
                    year=source.year,
                    month=source.month,
                    file_name=source.file_name,
                    file_path=str(source.file_path),
                )
                for r in records:
                    r["file_id"] = file_id

                count = self.loader.load_records(file_id, records)
                self.loader.mark_file_status(file_id, "success")
                stats["files_processed"] += 1
                stats["records_loaded"] += count
            except Exception as exc:
                stats["files_failed"] += 1
                logger.error("Failed %s: %s", source.file_name, exc, exc_info=True)
                self.loader.log_parse_error(source, str(exc))
                try:
                    self.loader.mark_file_status(file_id, "failed", str(exc))
                except Exception:
                    pass

        logger.info("Ingestion complete: %s", stats)
        return stats

    def reconcile(self) -> None:
        logger.info("Running reconciliation rules...")
        apply_premium_validation(self.db)
        apply_user_fees(self.db)
        apply_cancellation_window(self.db)

    def validate(self, issuer: str | None = None) -> None:
        run_load_validation(self.db, issuer)

    def report(self, issuer: str | None = None) -> None:
        run_kpi_reports(self.db, issuer)

    def export_assets(self, issuer: str | None = None) -> dict[str, int]:
        return export_assets(self.db.conn, issuer or settings.issuer_filter)

    def run_full(self, issuer: str | None = None) -> dict:
        stats = self.ingest_and_load()
        self.reconcile()
        self.validate(issuer or settings.issuer_filter)
        self.report(issuer or settings.issuer_filter)
        asset_stats = self.export_assets(issuer or settings.issuer_filter)
        stats["asset_partitions"] = asset_stats["partitions"]
        stats["asset_rollups"] = asset_stats["rollups"]
        return stats

    def close(self) -> None:
        self.db.close()
