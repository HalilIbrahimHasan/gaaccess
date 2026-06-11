"""
SFTP audit mode — recursively scan remote folders at any depth without downloading.

Reports valid .xml and .xml.gz files and compares remote inventory to local source_data.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from config.config import settings
from ingestion.sftp_file_classifier import local_xml_name_from_remote
from ingestion.sftp_ingestion import _normalize_month, list_remote_partitions
from ingestion.sftp_tree_walk import FolderScan, walk_partition_month
from reporting.csv_writer import write_csv
from utils.logger import get_logger

logger = get_logger(__name__)

AUDIT_COLUMNS = [
    "issuer", "year", "month", "folder_path", "depth", "parent_path", "remote_path",
    "total_files", "valid_xml_count", "valid_xml_gz_count",
    "valid_xml_names", "valid_xml_gz_names", "valid_remote_paths",
    "skipped_edi_count", "skipped_tracking_count", "skipped_report_count",
    "skipped_summary_count", "skipped_log_count", "skipped_to_count",
    "skipped_other_count", "status", "message",
]

SUMMARY_COLUMNS = [
    "issuer", "year", "month", "row_type", "folders_scanned_count",
    "max_depth_scanned", "total_valid_xml", "total_valid_xml_gz",
    "total_valid_files", "local_xml_files", "missing_difference",
    "sample_valid_paths", "message",
]


def _local_xml_names(local_root: Path, issuer: str, year: str, month: str) -> set[str]:
    local_month = local_root / issuer / year / _normalize_month(month)
    if not local_month.exists():
        return set()
    return {p.name for p in local_month.glob("*.xml") if p.is_file()}


def _folder_to_row(
    issuer: str,
    year: str,
    month: str,
    folder: FolderScan,
) -> dict:
    return {
        "issuer": issuer,
        "year": year,
        "month": _normalize_month(month),
        "folder_path": folder.folder_path,
        "depth": folder.depth,
        "parent_path": folder.parent_path,
        "remote_path": folder.folder_path,
        "total_files": folder.total_files,
        "valid_xml_count": folder.valid_xml_count,
        "valid_xml_gz_count": folder.valid_xml_gz_count,
        "valid_xml_names": ";".join(folder.valid_xml_names),
        "valid_xml_gz_names": ";".join(folder.valid_xml_gz_names),
        "valid_remote_paths": ";".join(folder.valid_remote_paths),
        "skipped_edi_count": folder.skipped_edi_count,
        "skipped_tracking_count": folder.skipped_tracking_count,
        "skipped_report_count": folder.skipped_report_count,
        "skipped_summary_count": folder.skipped_summary_count,
        "skipped_log_count": folder.skipped_log_count,
        "skipped_to_count": folder.skipped_to_count,
        "skipped_other_count": folder.skipped_other_count,
        "status": folder.status,
        "message": folder.message,
    }


def _summary_row(
    issuer: str,
    year: str,
    month: str,
    walk,
    local_names: set[str],
) -> dict:
    remote_names = walk.all_valid_local_names()
    missing = remote_names - local_names
    total_valid = walk.total_valid_xml + walk.total_valid_xml_gz
    return {
        "issuer": issuer,
        "year": year,
        "month": _normalize_month(month),
        "row_type": "PARTITION_SUMMARY",
        "folders_scanned_count": walk.folders_scanned_count,
        "max_depth_scanned": walk.max_depth_scanned,
        "total_valid_xml": walk.total_valid_xml,
        "total_valid_xml_gz": walk.total_valid_xml_gz,
        "total_valid_files": total_valid,
        "local_xml_files": len(local_names),
        "missing_difference": len(missing),
        "sample_valid_paths": ";".join(walk.sample_valid_paths),
        "message": (
            f"Scanned {walk.folders_scanned_count} folder(s) to depth "
            f"{walk.max_depth_scanned}; {total_valid} valid remote file(s); "
            f"{len(missing)} missing locally"
        ),
    }


def audit_partition(
    sftp,
    remote_root: str,
    issuer: str,
    year: str,
    month: str,
    local_root: Path,
) -> tuple[list[dict], list[dict]]:
    """Audit one issuer/year/month partition. Returns folder rows and summary rows."""
    month_path = f"{remote_root.rstrip('/')}/{issuer}/{year}/{month}"
    walk = walk_partition_month(sftp, month_path, issuer)
    folder_rows = [_folder_to_row(issuer, year, month, f) for f in walk.folders]

    local_names = _local_xml_names(local_root, issuer, year, month)
    summary = _summary_row(issuer, year, month, walk, local_names)

    logger.info(
        "SFTP AUDIT SUMMARY %s/%s/%s | folders_scanned=%d | max_depth=%d | "
        "valid_xml=%d | valid_gz=%d | local_xml=%d | missing=%d",
        issuer, year, _normalize_month(month),
        walk.folders_scanned_count,
        walk.max_depth_scanned,
        walk.total_valid_xml,
        walk.total_valid_xml_gz,
        len(local_names),
        summary["missing_difference"],
    )
    if walk.sample_valid_paths:
        logger.info(
            "SFTP AUDIT sample valid paths %s/%s/%s: %s",
            issuer, year, _normalize_month(month),
            "; ".join(walk.sample_valid_paths[:5]),
        )
    if summary["missing_difference"] > 0:
        missing = sorted(walk.all_valid_local_names() - local_names)
        logger.warning(
            "SFTP AUDIT %s/%s/%s missing local XML (%d): %s",
            issuer, year, _normalize_month(month),
            len(missing),
            "; ".join(missing[:20]) + (" ..." if len(missing) > 20 else ""),
        )

    return folder_rows, [summary]


def run_sftp_audit(
    host: str,
    port: int,
    username: str,
    password: str,
    remote_root: str,
    local_root: Path | None = None,
    issuer_filter: str | None = None,
    year_filter: str | None = None,
    month_allowlist: list[str] | None = None,
    output_csv: Path | None = None,
) -> Path:
    """
    Connect to SFTP, recursively audit remote folders, write CSV report.

    Does not download or modify any local files.
    """
    import paramiko

    local_root = local_root or settings.source_data_path
    month_allowlist = month_allowlist or settings.sftp_audit_months()
    allowset = {str(int(m)).zfill(2) for m in month_allowlist}
    output_csv = output_csv or (settings.reports_path / "sftp_audit_03_04_05_06.csv")

    logger.info(
        "SFTP AUDIT ONLY — months=%s recursive unlimited depth (no download)",
        sorted(allowset),
    )

    transport = paramiko.Transport((host, port))
    folder_rows: list[dict] = []
    summary_rows: list[dict] = []

    try:
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        partitions = list_remote_partitions(
            sftp, remote_root, issuer_filter, year_filter, month_filter=None,
        )
        partitions = [p for p in partitions if _normalize_month(p[2]) in allowset]
        if year_filter:
            partitions = [p for p in partitions if p[1] == year_filter]
        if month_allowlist and not partitions:
            logger.warning(
                "SFTP audit: no remote partitions matched months %s", sorted(allowset),
            )

        for issuer, year, month in partitions:
            folders, summaries = audit_partition(
                sftp, remote_root, issuer, year, month, local_root,
            )
            folder_rows.extend(folders)
            summary_rows.extend(summaries)

        sftp.close()
    finally:
        transport.close()

    folder_df = pd.DataFrame(folder_rows, columns=AUDIT_COLUMNS)
    summary_df = pd.DataFrame(summary_rows, columns=SUMMARY_COLUMNS)
    write_csv(folder_df, output_csv)
    summary_path = output_csv.with_name(output_csv.stem + "_summary.csv")
    write_csv(summary_df, summary_path)

    logger.info("=" * 60)
    logger.info("SFTP AUDIT COMPLETE — %s", output_csv)
    logger.info("Folder rows: %d | Summary rows: %d", len(folder_df), len(summary_df))
    logger.info("Summary file: %s", summary_path)
    for s in summary_rows:
        logger.info(
            "  %s/%s/%s | folders=%d | max_depth=%d | valid=%d (.xml=%d .gz=%d) | "
            "local=%d | missing=%d",
            s["issuer"], s["year"], s["month"],
            s["folders_scanned_count"],
            s["max_depth_scanned"],
            s["total_valid_files"],
            s["total_valid_xml"],
            s["total_valid_xml_gz"],
            s["local_xml_files"],
            s["missing_difference"],
        )
    logger.info("=" * 60)
    return output_csv
