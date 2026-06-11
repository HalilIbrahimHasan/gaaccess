"""
SFTP ingestion — download 834 XML from remote day/batch folders.

Remote layout:
    SFTP_ROOT/{issuer}/{year}/{month}/{day}/{batch_folder}/files

Local layout (unchanged):
    source_data/{issuer}/{year}/{month}/*.xml

Only downloads: from_{issuer}_GA_834_INDV_*.xml.gz
Skips: tracking, edi, dtl, summary, report, log, to_* files
"""

from __future__ import annotations

import gzip
from pathlib import Path

from config.config import settings
from ingestion.sftp_file_classifier import is_valid_834_gz, local_xml_name_from_gz
from utils.logger import get_logger

logger = get_logger(__name__)


def _issuer_ok(name: str) -> bool:
    return name.isdigit() and len(name) == 5


def _year_ok(name: str) -> bool:
    return name.isdigit() and len(name) == 4


def _month_ok(name: str) -> bool:
    if not name.isdigit() or len(name) > 2:
        return False
    return 1 <= int(name) <= 12


def _day_ok(name: str) -> bool:
    if not name.isdigit() or len(name) > 2:
        return False
    return 1 <= int(name) <= 31


def _normalize_month(name: str) -> str:
    return str(int(name)).zfill(2)


def list_remote_partitions(
    sftp,
    remote_root: str,
    issuer_filter: str | None = None,
    year_filter: str | None = None,
    month_filter: str | None = None,
) -> list[tuple[str, str, str]]:
    """List (issuer, year, month) tuples present on SFTP."""
    partitions: list[tuple[str, str, str]] = []
    root = remote_root.rstrip("/")

    try:
        issuer_dirs = sftp.listdir(root)
    except OSError as exc:
        logger.error("Cannot list SFTP root %s: %s", root, exc)
        return partitions

    for issuer in sorted(issuer_dirs):
        if not _issuer_ok(issuer):
            continue
        if issuer_filter and issuer != issuer_filter:
            continue
        issuer_path = f"{root}/{issuer}"
        try:
            year_dirs = sftp.listdir(issuer_path)
        except OSError:
            continue
        for year in sorted(year_dirs):
            if not _year_ok(year):
                continue
            if year_filter and year != year_filter:
                continue
            year_path = f"{issuer_path}/{year}"
            try:
                month_dirs = sftp.listdir(year_path)
            except OSError:
                continue
            for month in sorted(month_dirs):
                if not _month_ok(month):
                    continue
                month_norm = _normalize_month(month)
                if month_filter and month_norm != str(month_filter).zfill(2):
                    continue
                # Keep remote folder name as-is (may be "2" or "02"); normalize for local path
                partitions.append((issuer, year, month))

    logger.info("SFTP partitions to sync: %d", len(partitions))
    for p in partitions:
        logger.info("  remote partition: %s/%s/%s", *p)
    return partitions


def _list_dirs(sftp, path: str) -> list[str]:
    try:
        attrs = sftp.listdir_attr(path)
        return sorted(
            a.filename for a in attrs
            if not a.filename.startswith(".") and _is_dir(sftp, f"{path}/{a.filename}")
        )
    except OSError:
        return []


def _is_dir(sftp, path: str) -> bool:
    import stat
    try:
        return stat.S_ISDIR(sftp.stat(path).st_mode)
    except OSError:
        return False


def _list_files(sftp, path: str) -> list[str]:
    try:
        attrs = sftp.listdir_attr(path)
        return sorted(
            a.filename for a in attrs
            if not a.filename.startswith(".") and not _is_dir(sftp, f"{path}/{a.filename}")
        )
    except OSError:
        return []


def download_partition(
    sftp,
    remote_root: str,
    issuer: str,
    year: str,
    month: str,
    local_root: Path,
) -> list[Path]:
    """
    Download all valid .xml.gz files from remote day/batch folders.

    Flattens into source_data/{issuer}/{year}/{month}/*.xml
    """
    month_path = f"{remote_root.rstrip('/')}/{issuer}/{year}/{month}"
    local_month = local_root / issuer / year / _normalize_month(month)
    local_month.mkdir(parents=True, exist_ok=True)

    downloaded: list[Path] = []
    day_dirs = _list_dirs(sftp, month_path)

    if not day_dirs:
        logger.warning("No day folders under %s", month_path)
        return downloaded

    for day in day_dirs:
        if not _day_ok(day):
            logger.debug("Skip non-day folder: %s", day)
            continue
        day_path = f"{month_path}/{day}"
        batch_dirs = _list_dirs(sftp, day_path)

        for batch in batch_dirs:
            batch_path = f"{day_path}/{batch}"
            files = _list_files(sftp, batch_path)

            for filename in files:
                if not is_valid_834_gz(issuer, filename):
                    continue

                remote_file = f"{batch_path}/{filename}"
                local_xml_name = local_xml_name_from_gz(filename)
                local_path = local_month / local_xml_name

                try:
                    with sftp.open(remote_file, "rb") as remote_f:
                        compressed = remote_f.read()
                    xml_bytes = gzip.decompress(compressed)
                    local_path.write_bytes(xml_bytes)
                    downloaded.append(local_path)
                    logger.info(
                        "SFTP downloaded: %s → %s (%d bytes)",
                        remote_file, local_path, len(xml_bytes),
                    )
                except Exception as exc:
                    logger.error("Failed to download %s: %s", remote_file, exc)

    logger.info(
        "Partition %s/%s/%s: %d XML file(s) saved to %s",
        issuer, year, month, len(downloaded), local_month,
    )
    return downloaded


def ingest_from_sftp(
    host: str,
    port: int,
    username: str,
    password: str,
    remote_root: str,
    local_root: Path | None = None,
    issuer_filter: str | None = None,
    year_filter: str | None = None,
    month_filter: str | None = None,
) -> int:
    """
    Connect to SFTP, download valid XML files, flatten into source_data.

    Returns total number of XML files downloaded.
    """
    import paramiko

    local_root = local_root or settings.source_data_path
    local_root.mkdir(parents=True, exist_ok=True)

    transport = paramiko.Transport((host, port))
    try:
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        partitions = list_remote_partitions(
            sftp, remote_root, issuer_filter, year_filter, month_filter,
        )
        total = 0
        for issuer, year, month in partitions:
            files = download_partition(sftp, remote_root, issuer, year, month, local_root)
            total += len(files)

        sftp.close()
        logger.info("SFTP ingestion complete: %d file(s) downloaded", total)
        return total
    finally:
        transport.close()
