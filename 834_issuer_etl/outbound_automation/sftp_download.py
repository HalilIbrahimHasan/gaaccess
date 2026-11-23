"""
Outbound SFTP audit / download.

Remote:  SFTP_OUTBOUND_REMOTE_PATH/{issuer}/{year}/{month}/** (unlimited depth, e.g. day folders)
Select:  TO files only (outbound_automation.file_rules)
Local:   <outbound source root>/{issuer}/{year}/{MM}/*.xml (flattened, same as inbound layout)

Reuses the proven inbound SFTP helpers for partition listing, directory listing and
gz/xz decompression. The inbound FROM-file walker/classifier is not used.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
import paramiko

from ingestion.sftp_file_classifier import local_xml_name_from_remote
from ingestion.sftp_filters import log_effective_filters
from ingestion.sftp_ingestion import (
    _download_remote_file,
    _normalize_month,
    list_remote_partitions,
)
from ingestion.sftp_tree_walk import _list_dirs, _list_files
from outbound_automation.config import SftpCredentials
from outbound_automation.file_rules import classify_outbound_filename
from utils.logger import get_logger

logger = get_logger(__name__)

_FORMAT_BY_CLASS = {"valid_xml": "xml", "valid_gz": "gz", "valid_xz": "xz"}


@dataclass
class OutboundPartitionResult:
    issuer: str
    year: str
    month: str
    folders_scanned: int = 0
    max_depth: int = 0
    files_scanned: int = 0
    class_counts: dict[str, int] = field(default_factory=dict)
    valid_files: list[dict[str, Any]] = field(default_factory=list)
    file_manifest: list[dict[str, Any]] = field(default_factory=list)
    downloaded: int = 0
    existing: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def valid_total(self) -> int:
        return len(self.valid_files)

    def to_row(self) -> dict[str, Any]:
        row: dict[str, Any] = {
            "issuer": self.issuer,
            "year": self.year,
            "month": _normalize_month(self.month),
            "folders_scanned": self.folders_scanned,
            "max_depth": self.max_depth,
            "files_scanned": self.files_scanned,
            "valid_to_total": self.valid_total,
            "downloaded": self.downloaded,
            "existing": self.existing,
            "failed": self.failed,
            "errors": "; ".join(self.errors),
        }
        for cls in (
            "valid_xml", "valid_gz", "valid_xz", "from",
            "edi", "tracking", "report", "summary", "log", "other",
        ):
            row[f"count_{cls}"] = self.class_counts.get(cls, 0)
        return row


def _walk_folder(sftp, remote_path: str, depth: int, result: OutboundPartitionResult) -> None:
    result.folders_scanned += 1
    result.max_depth = max(result.max_depth, depth)

    for filename in _list_files(sftp, remote_path):
        result.files_scanned += 1
        cls = classify_outbound_filename(filename)
        result.class_counts[cls] = result.class_counts.get(cls, 0) + 1
        remote_file = f"{remote_path}/{filename}"
        selected = cls in _FORMAT_BY_CLASS
        result.file_manifest.append(
            {
                "issuer": result.issuer,
                "year": result.year,
                "month": _normalize_month(result.month),
                "depth": depth,
                "remote_path": remote_file,
                "filename": filename,
                "classification": cls,
                "selected_as_to_file": selected,
            }
        )
        if selected:
            result.valid_files.append(
                {
                    "remote_path": remote_file,
                    "filename": filename,
                    "local_name": local_xml_name_from_remote(filename),
                    "format": _FORMAT_BY_CLASS[cls],
                }
            )

    for subdir in _list_dirs(sftp, remote_path):
        _walk_folder(sftp, f"{remote_path}/{subdir}", depth + 1, result)


def walk_outbound_partition(
    sftp, remote_root: str, issuer: str, year: str, month: str,
) -> OutboundPartitionResult:
    month_path = f"{remote_root.rstrip('/')}/{issuer}/{year}/{month}"
    result = OutboundPartitionResult(issuer=issuer, year=year, month=month)
    try:
        sftp.stat(month_path)
    except OSError as exc:
        msg = f"Partition path not found: {month_path} ({exc})"
        logger.warning(msg)
        result.errors.append(msg)
        return result

    _walk_folder(sftp, month_path, 0, result)
    logger.info(
        "Outbound partition %s/%s/%s: folders=%d max_depth=%d files=%d to_selected=%d",
        issuer, year, month, result.folders_scanned, result.max_depth,
        result.files_scanned, result.valid_total,
    )
    return result


def download_outbound_partition(
    sftp,
    result: OutboundPartitionResult,
    local_root: Path,
    *,
    force_download: bool,
    keep_compressed: bool,
) -> None:
    local_month = local_root / result.issuer / result.year / _normalize_month(result.month)
    local_month.mkdir(parents=True, exist_ok=True)
    for entry in result.valid_files:
        local_path = local_month / entry["local_name"]
        if local_path.exists() and not force_download:
            result.existing += 1
            continue
        if _download_remote_file(
            sftp,
            entry["remote_path"],
            entry["filename"],
            local_path,
            keep_compressed=keep_compressed,
        ):
            result.downloaded += 1
        else:
            result.failed += 1


def _export(results: list[OutboundPartitionResult], output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "outbound_sftp_partition_summary.csv"
    manifest_path = output_dir / "outbound_sftp_file_manifest.csv"
    pd.DataFrame([r.to_row() for r in results]).to_csv(summary_path, index=False)
    manifest_rows = [m for r in results for m in r.file_manifest]
    pd.DataFrame(
        manifest_rows,
        columns=[
            "issuer", "year", "month", "depth", "remote_path",
            "filename", "classification", "selected_as_to_file",
        ],
    ).to_csv(manifest_path, index=False)
    logger.info("Wrote %s and %s", summary_path, manifest_path)
    return {"partition_summary": summary_path, "file_manifest": manifest_path}


def run_outbound_sftp(
    *,
    creds: SftpCredentials,
    remote_root: str,
    local_root: Path,
    output_dir: Path,
    issuer_allow: set[str] | None,
    year_allow: set[str] | None,
    month_allow: set[str] | None,
    download: bool,
    force_download: bool = False,
    keep_compressed: bool = False,
) -> tuple[list[OutboundPartitionResult], dict[str, Path]]:
    """Audit (download=False) or download (download=True) outbound TO files."""
    log_effective_filters(issuer_allow, year_allow, month_allow)
    logger.info(
        "OUTBOUND SFTP %s — remote_root=%s local_root=%s",
        "DOWNLOAD" if download else "AUDIT ONLY (no download)",
        remote_root,
        local_root,
    )

    results: list[OutboundPartitionResult] = []
    transport = paramiko.Transport((creds.host, creds.port))
    try:
        transport.connect(username=creds.username, password=creds.password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        partitions = list_remote_partitions(
            sftp, remote_root, issuer_allow, year_allow, month_allow,
        )
        if not partitions:
            logger.warning("Outbound SFTP: no remote partitions matched filters")
        for issuer, year, month in partitions:
            result = walk_outbound_partition(sftp, remote_root, issuer, year, month)
            if download:
                download_outbound_partition(
                    sftp, result, local_root,
                    force_download=force_download,
                    keep_compressed=keep_compressed,
                )
            results.append(result)
        sftp.close()
    finally:
        transport.close()

    paths = _export(results, output_dir)
    return results, paths
