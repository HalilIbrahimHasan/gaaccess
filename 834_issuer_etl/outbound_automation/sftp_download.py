"""
Outbound SFTP audit / download.

Remote:  SFTP_OUTBOUND_REMOTE_PATH/{issuer}/{year}/{month}/** (unlimited depth, e.g. day folders)
Select:  TO files only (outbound_automation.file_rules)
Local:   <outbound source root>/{issuer}/{year}/{MM}/*.xml (flattened, same as inbound layout)

Every remote call goes through OutboundSftpSession (timeouts, SFTP_OP_START/END
logging, no-progress watchdog, bounded reconnect/retry). Files are transferred by
outbound_automation.sftp_transfer (completed-file skip, .part files, atomic
finalize, per-month download state). The shared partition lister and filename
helpers are reused read-only; the inbound downloader itself is not used.

Reports are rewritten after every month, so an interrupted run still leaves the
manifest/summary/download log of everything processed so far.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from ingestion.sftp_file_classifier import local_xml_name_from_remote
from ingestion.sftp_filters import log_effective_filters
from ingestion.sftp_ingestion import _normalize_month, list_remote_partitions
from outbound_automation.config import SftpCredentials, SftpReliabilitySettings
from outbound_automation.file_rules import ALL_CLASSES, classify_outbound_filename
from outbound_automation.sftp_session import (
    Connector,
    OutboundSftpSession,
    SessionStats,
    SftpOperationFailed,
    SftpReconnectLimitError,
    describe_error,
    paramiko_connector,
)
from outbound_automation.sftp_transfer import (
    ACTION_DOWNLOADED,
    ACTION_FAILED,
    ACTION_REDOWNLOADED,
    ACTION_SKIP,
    DOWNLOAD_LOG_COLUMNS,
    REASON_RECONNECT_LIMIT,
    DownloadStateStore,
    FileOutcome,
    RemoteFile,
    process_remote_file,
)
from utils.logger import get_logger

logger = get_logger(__name__)

_FORMAT_BY_CLASS = {"valid_xml": "xml", "valid_gz": "gz", "valid_xz": "xz"}
_TYPE_MASK = 0o170000
_DIR_MODE = 0o040000

MANIFEST_COLUMNS = [
    "issuer", "year", "month", "depth", "remote_path",
    "filename", "classification", "selected_as_to_file",
]


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
    redownloaded: int = 0
    failed: int = 0
    listing_failed: bool = False
    errors: list[str] = field(default_factory=list)
    download_log: list[dict[str, Any]] = field(default_factory=list)

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
            "redownloaded": self.redownloaded,
            "failed": self.failed,
            "listing_failed": self.listing_failed,
            "errors": "; ".join(self.errors),
        }
        for cls in ALL_CLASSES:
            row[f"count_{cls}"] = self.class_counts.get(cls, 0)
        return row


@dataclass
class OutboundSftpRun:
    results: list[OutboundPartitionResult]
    paths: dict[str, Path]
    stats: SessionStats
    run_errors: list[str] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        return bool(self.run_errors) or any(r.failed or r.listing_failed for r in self.results)


class _PartitionLister:
    """Hands the shared partition lister a client whose listdir is logged, timed and retried."""

    def __init__(self, session: OutboundSftpSession) -> None:
        self._session = session

    def listdir(self, path: str) -> list[str]:
        return self._session.listdir(path)


def _is_dir(attr: Any) -> bool:
    mode = getattr(attr, "st_mode", None)
    return mode is not None and (mode & _TYPE_MASK) == _DIR_MODE


def _list_entries(session: OutboundSftpSession, path: str) -> tuple[list[Any], list[str]]:
    """(files, subdirectory names), both sorted by name, from a single listdir_attr."""
    try:
        entries = session.listdir_attr(path)
    except OSError as exc:
        logger.warning("Cannot list directory %s: %s", path, exc)
        return [], []
    files = sorted((e for e in entries if not _is_dir(e)), key=lambda e: e.filename)
    dirs = sorted(e.filename for e in entries if _is_dir(e))
    return files, dirs


def _walk_folder(
    session: OutboundSftpSession, remote_path: str, depth: int, result: OutboundPartitionResult,
) -> None:
    result.folders_scanned += 1
    result.max_depth = max(result.max_depth, depth)

    files, subdirs = _list_entries(session, remote_path)
    for attr in files:
        filename = attr.filename
        result.files_scanned += 1
        cls = classify_outbound_filename(filename)
        result.class_counts[cls] = result.class_counts.get(cls, 0) + 1
        if cls == "to_nonstandard":
            logger.warning(
                "Unrecognized TO filename reported, not selected: %s/%s", remote_path, filename,
            )
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
                    "remote_size": getattr(attr, "st_size", None),
                    "remote_mtime": getattr(attr, "st_mtime", None),
                }
            )

    for subdir in subdirs:
        _walk_folder(session, f"{remote_path}/{subdir}", depth + 1, result)


def walk_outbound_partition(
    session: OutboundSftpSession, remote_root: str, issuer: str, year: str, month: str,
) -> OutboundPartitionResult:
    """List one month. SftpOperationFailed propagates when retries are exhausted."""
    month_path = f"{remote_root.rstrip('/')}/{issuer}/{year}/{month}"
    result = OutboundPartitionResult(issuer=issuer, year=year, month=month)
    try:
        session.stat(month_path)
    except OSError as exc:
        msg = f"Partition path not found: {month_path} ({exc})"
        logger.warning(msg)
        result.errors.append(msg)
        return result

    _walk_folder(session, month_path, 0, result)
    logger.info(
        "Outbound partition %s/%s/%s: folders=%d max_depth=%d files=%d to_selected=%d",
        issuer, year, month, result.folders_scanned, result.max_depth,
        result.files_scanned, result.valid_total,
    )
    return result


def _tally(result: OutboundPartitionResult, outcome: FileOutcome) -> None:
    if outcome.action == ACTION_SKIP:
        result.existing += 1
    elif outcome.action == ACTION_DOWNLOADED:
        result.downloaded += 1
    elif outcome.action == ACTION_REDOWNLOADED:
        result.redownloaded += 1
    else:
        result.failed += 1
        result.errors.append(f"{outcome.filename}: {outcome.error}")
    result.download_log.append(
        {
            "issuer": result.issuer,
            "year": result.year,
            "month": _normalize_month(result.month),
            **outcome.as_row(),
        }
    )


def download_outbound_partition(
    session: OutboundSftpSession,
    result: OutboundPartitionResult,
    local_root: Path,
    *,
    force_download: bool,
    keep_compressed: bool,
    settings: SftpReliabilitySettings,
) -> None:
    local_month = local_root / result.issuer / result.year / _normalize_month(result.month)
    local_month.mkdir(parents=True, exist_ok=True)
    store = DownloadStateStore(local_month)
    total = len(result.valid_files)
    logger.info(
        "Outbound download %s/%s/%s: %d TO file(s) -> %s",
        result.issuer, result.year, result.month, total, local_month,
    )
    for index, entry in enumerate(result.valid_files, start=1):
        remote = RemoteFile.from_entry(entry)
        try:
            outcome = process_remote_file(
                session, remote, local_month, store,
                settings=settings,
                force_download=force_download,
                keep_compressed=keep_compressed,
                index=index,
                total=total,
            )
        except SftpReconnectLimitError as exc:
            _tally(result, FileOutcome(
                index, total, remote.filename, remote.local_name, ACTION_FAILED,
                REASON_RECONNECT_LIMIT, remote.size, None, error=describe_error(exc),
            ))
            raise
        _tally(result, outcome)
    logger.info(
        "Outbound download %s/%s/%s done: downloaded=%d skipped=%d redownloaded=%d failed=%d",
        result.issuer, result.year, result.month,
        result.downloaded, result.existing, result.redownloaded, result.failed,
    )


def _export(results: list[OutboundPartitionResult], output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "outbound_sftp_partition_summary.csv"
    manifest_path = output_dir / "outbound_sftp_file_manifest.csv"
    download_log_path = output_dir / "outbound_sftp_download_log.csv"
    pd.DataFrame([r.to_row() for r in results]).to_csv(summary_path, index=False)
    pd.DataFrame(
        [m for r in results for m in r.file_manifest], columns=MANIFEST_COLUMNS,
    ).to_csv(manifest_path, index=False)
    pd.DataFrame(
        [row for r in results for row in r.download_log], columns=list(DOWNLOAD_LOG_COLUMNS),
    ).to_csv(download_log_path, index=False)
    return {
        "partition_summary": summary_path,
        "file_manifest": manifest_path,
        "download_log": download_log_path,
    }


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
    settings: SftpReliabilitySettings | None = None,
    connector: Connector | None = None,
    sleep: Callable[[float], Any] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    start_watchdog: bool = True,
) -> OutboundSftpRun:
    """Audit (download=False) or download (download=True) outbound TO files."""
    settings = settings or SftpReliabilitySettings()
    log_effective_filters(issuer_allow, year_allow, month_allow)
    logger.info(
        "OUTBOUND SFTP %s — remote_root=%s local_root=%s",
        "DOWNLOAD" if download else "AUDIT ONLY (no download)",
        remote_root,
        local_root,
    )
    logger.info(
        "OUTBOUND SFTP reliability: %s",
        ", ".join(f"{label}={value}" for label, value in settings.describe()),
    )

    session = OutboundSftpSession(
        connector or paramiko_connector(creds, settings),
        settings,
        sleep=sleep,
        clock=clock,
        start_watchdog=start_watchdog,
    )
    results: list[OutboundPartitionResult] = []
    run_errors: list[str] = []
    try:
        partitions = list_remote_partitions(
            _PartitionLister(session), remote_root, issuer_allow, year_allow, month_allow,
        )
        if not partitions:
            logger.warning("Outbound SFTP: no remote partitions matched filters")
        for p_index, (issuer, year, month) in enumerate(partitions, start=1):
            logger.info("PARTITION_START [%d/%d] %s/%s/%s", p_index, len(partitions), issuer, year, month)
            try:
                result = walk_outbound_partition(session, remote_root, issuer, year, month)
            except SftpOperationFailed as exc:
                result = OutboundPartitionResult(
                    issuer=issuer, year=year, month=month, listing_failed=True,
                )
                result.errors.append(f"Listing failed: {describe_error(exc)}")
                logger.error("Outbound partition %s/%s/%s listing failed; not downloaded: %s",
                             issuer, year, month, describe_error(exc))
                results.append(result)
                _export(results, output_dir)
                continue
            results.append(result)
            _export(results, output_dir)
            if download:
                try:
                    download_outbound_partition(
                        session, result, local_root,
                        force_download=force_download,
                        keep_compressed=keep_compressed,
                        settings=settings,
                    )
                finally:
                    _export(results, output_dir)
    except SftpReconnectLimitError as exc:
        msg = f"Run stopped: {describe_error(exc)}"
        logger.error(msg)
        run_errors.append(msg)
    except SftpOperationFailed as exc:
        msg = f"Remote partition discovery failed: {describe_error(exc)}"
        logger.error(msg)
        run_errors.append(msg)
    finally:
        session.close()

    paths = _export(results, output_dir)
    logger.info("Wrote %s", ", ".join(str(p) for p in paths.values()))
    return OutboundSftpRun(results=results, paths=paths, stats=session.stats, run_errors=run_errors)
