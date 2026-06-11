"""
SFTP audit mode — scan remote day/batch folders without downloading.

Reports valid vs skipped files and compares remote inventory to local source_data.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from config.config import settings
from ingestion.sftp_file_classifier import classify_sftp_filename, local_xml_name_from_gz
from ingestion.sftp_ingestion import (
    _day_ok,
    _list_dirs,
    _list_files,
    _normalize_month,
    list_remote_partitions,
)
from reporting.csv_writer import write_csv
from utils.logger import get_logger

logger = get_logger(__name__)

AUDIT_COLUMNS = [
    "issuer", "year", "month", "day", "batch_folder", "remote_path",
    "total_files", "valid_xml_gz_count", "valid_xml_gz_names",
    "skipped_edi_count", "skipped_tracking_count", "skipped_report_count",
    "skipped_summary_count", "skipped_log_count", "skipped_to_count",
    "skipped_other_count", "status", "message",
]

SKIP_COUNT_COLS = {
    "edi": "skipped_edi_count",
    "tracking": "skipped_tracking_count",
    "report": "skipped_report_count",
    "summary": "skipped_summary_count",
    "log": "skipped_log_count",
    "to": "skipped_to_count",
    "other": "skipped_other_count",
}


def _local_xml_names(local_root: Path, issuer: str, year: str, month: str) -> set[str]:
    local_month = local_root / issuer / year / _normalize_month(month)
    if not local_month.exists():
        return set()
    return {p.name for p in local_month.glob("*.xml") if p.is_file()}


def _audit_batch_folder(
    sftp,
    issuer: str,
    year: str,
    month: str,
    day: str,
    batch: str,
    batch_path: str,
) -> dict:
    row = {col: "" for col in AUDIT_COLUMNS}
    row.update({
        "issuer": issuer,
        "year": year,
        "month": _normalize_month(month),
        "day": day,
        "batch_folder": batch,
        "remote_path": batch_path,
        "skipped_edi_count": 0,
        "skipped_tracking_count": 0,
        "skipped_report_count": 0,
        "skipped_summary_count": 0,
        "skipped_log_count": 0,
        "skipped_to_count": 0,
        "skipped_other_count": 0,
    })

    try:
        files = _list_files(sftp, batch_path)
    except Exception as exc:
        row["status"] = "SCAN_ERROR"
        row["message"] = str(exc)
        return row

    row["total_files"] = len(files)
    if not files:
        row["status"] = "EMPTY_BATCH"
        row["message"] = "Batch folder contains no files"
        row["valid_xml_gz_count"] = 0
        row["valid_xml_gz_names"] = ""
        return row

    valid_names: list[str] = []
    for filename in files:
        category = classify_sftp_filename(issuer, filename)
        if category == "valid":
            valid_names.append(filename)
        elif category in SKIP_COUNT_COLS:
            col = SKIP_COUNT_COLS[category]
            row[col] = int(row[col]) + 1

    row["valid_xml_gz_count"] = len(valid_names)
    row["valid_xml_gz_names"] = ";".join(valid_names)

    if valid_names:
        row["status"] = "HAS_VALID_XML"
        row["message"] = f"Found {len(valid_names)} valid XML.gz file(s)"
    else:
        row["status"] = "NO_VALID_XML"
        row["message"] = "No valid from_{issuer}_GA_834_INDV_*.xml.gz files in batch"

    return row


def audit_partition(
    sftp,
    remote_root: str,
    issuer: str,
    year: str,
    month: str,
    local_root: Path,
) -> tuple[list[dict], dict]:
    """Audit one issuer/year/month partition. Returns rows and summary stats."""
    month_path = f"{remote_root.rstrip('/')}/{issuer}/{year}/{month}"
    rows: list[dict] = []
    summary = {
        "issuer": issuer,
        "year": year,
        "month": _normalize_month(month),
        "batch_folders_scanned": 0,
        "remote_valid_xml_gz": 0,
        "local_xml_files": 0,
        "missing_difference": 0,
        "batches_with_valid_xml": 0,
        "batches_without_valid_xml": 0,
    }

    day_dirs = _list_dirs(sftp, month_path)
    if not day_dirs:
        logger.warning("SFTP audit: no day folders under %s", month_path)
        rows.append({
            **{col: "" for col in AUDIT_COLUMNS},
            "issuer": issuer,
            "year": year,
            "month": _normalize_month(month),
            "remote_path": month_path,
            "status": "NO_DAY_FOLDERS",
            "message": "Month folder exists but contains no day subfolders",
            "total_files": 0,
            "valid_xml_gz_count": 0,
            "valid_xml_gz_names": "",
            "skipped_edi_count": 0,
            "skipped_tracking_count": 0,
            "skipped_report_count": 0,
            "skipped_summary_count": 0,
            "skipped_log_count": 0,
            "skipped_to_count": 0,
            "skipped_other_count": 0,
        })
        return rows, summary

    remote_valid_names: set[str] = set()
    for day in day_dirs:
        if not _day_ok(day):
            continue
        day_path = f"{month_path}/{day}"
        batch_dirs = _list_dirs(sftp, day_path)
        for batch in batch_dirs:
            batch_path = f"{day_path}/{batch}"
            row = _audit_batch_folder(sftp, issuer, year, month, day, batch, batch_path)
            rows.append(row)
            summary["batch_folders_scanned"] += 1
            valid_count = int(row.get("valid_xml_gz_count") or 0)
            summary["remote_valid_xml_gz"] += valid_count
            if valid_count > 0:
                summary["batches_with_valid_xml"] += 1
                for name in str(row.get("valid_xml_gz_names") or "").split(";"):
                    if name:
                        remote_valid_names.add(local_xml_name_from_gz(name))
            else:
                summary["batches_without_valid_xml"] += 1

    local_names = _local_xml_names(local_root, issuer, year, month)
    summary["local_xml_files"] = len(local_names)
    summary["missing_difference"] = len(remote_valid_names - local_names)

    logger.info(
        "SFTP AUDIT SUMMARY %s/%s/%s | batches=%d | remote_valid_gz=%d | "
        "local_xml=%d | missing=%d | batches_with_valid=%d | batches_without_valid=%d",
        issuer, year, _normalize_month(month),
        summary["batch_folders_scanned"],
        summary["remote_valid_xml_gz"],
        summary["local_xml_files"],
        summary["missing_difference"],
        summary["batches_with_valid_xml"],
        summary["batches_without_valid_xml"],
    )
    if summary["missing_difference"] > 0:
        missing = sorted(remote_valid_names - local_names)
        logger.warning(
            "SFTP AUDIT %s/%s/%s missing local XML (%d): %s",
            issuer, year, _normalize_month(month),
            len(missing),
            "; ".join(missing[:20]) + (" ..." if len(missing) > 20 else ""),
        )

    return rows, summary


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
    Connect to SFTP, audit remote batch folders, write CSV report.

    Does not download or modify any local files.
    """
    import paramiko

    local_root = local_root or settings.source_data_path
    month_allowlist = month_allowlist or settings.sftp_audit_months()
    allowset = {str(int(m)).zfill(2) for m in month_allowlist}
    output_csv = output_csv or (settings.reports_path / "sftp_audit_03_04_05_06.csv")

    logger.info("SFTP AUDIT ONLY — months=%s (no download)", sorted(allowset))

    transport = paramiko.Transport((host, port))
    all_rows: list[dict] = []
    summaries: list[dict] = []

    try:
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        partitions = list_remote_partitions(
            sftp, remote_root, issuer_filter, year_filter, month_filter=None,
        )
        partitions = [
            p for p in partitions
            if _normalize_month(p[2]) in allowset
        ]
        if year_filter:
            partitions = [p for p in partitions if p[1] == year_filter]
        if month_allowlist and not partitions:
            logger.warning(
                "SFTP audit: no remote partitions matched months %s", sorted(allowset),
            )

        for issuer, year, month in partitions:
            rows, summary = audit_partition(
                sftp, remote_root, issuer, year, month, local_root,
            )
            all_rows.extend(rows)
            summaries.append(summary)

        sftp.close()
    finally:
        transport.close()

    df = pd.DataFrame(all_rows, columns=AUDIT_COLUMNS)
    write_csv(df, output_csv)

    logger.info("=" * 60)
    logger.info("SFTP AUDIT COMPLETE — %s", output_csv)
    logger.info("Batch rows written: %d", len(df))
    for s in summaries:
        logger.info(
            "  %s/%s/%s | batches_scanned=%d | remote_valid_gz=%d | "
            "local_xml=%d | missing=%d",
            s["issuer"], s["year"], s["month"],
            s["batch_folders_scanned"],
            s["remote_valid_xml_gz"],
            s["local_xml_files"],
            s["missing_difference"],
        )
    logger.info("=" * 60)
    return output_csv
