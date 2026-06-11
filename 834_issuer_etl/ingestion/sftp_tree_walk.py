"""
Recursive SFTP tree walk under issuer/year/month.

Does not assume fixed depth — walks every subfolder until exhausted.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ingestion.sftp_file_classifier import classify_sftp_filename, local_xml_name_from_remote
from utils.logger import get_logger

logger = get_logger(__name__)

SKIP_COUNT_KEYS = {
    "edi": "skipped_edi_count",
    "tracking": "skipped_tracking_count",
    "report": "skipped_report_count",
    "summary": "skipped_summary_count",
    "log": "skipped_log_count",
    "to": "skipped_to_count",
    "other": "skipped_other_count",
}


@dataclass
class FolderScan:
    """Scan result for one remote folder at any depth."""

    folder_path: str
    depth: int
    parent_path: str
    total_files: int = 0
    valid_xml_count: int = 0
    valid_xml_gz_count: int = 0
    valid_xml_names: list[str] = field(default_factory=list)
    valid_xml_gz_names: list[str] = field(default_factory=list)
    valid_remote_paths: list[str] = field(default_factory=list)
    skipped_edi_count: int = 0
    skipped_tracking_count: int = 0
    skipped_report_count: int = 0
    skipped_summary_count: int = 0
    skipped_log_count: int = 0
    skipped_to_count: int = 0
    skipped_other_count: int = 0
    status: str = ""
    message: str = ""

    def all_valid_local_names(self) -> list[str]:
        names = [local_xml_name_from_remote(n) for n in self.valid_xml_names]
        names.extend(local_xml_name_from_remote(n) for n in self.valid_xml_gz_names)
        return names


@dataclass
class PartitionWalkResult:
    """Full recursive scan of one issuer/year/month."""

    month_path: str
    folders: list[FolderScan] = field(default_factory=list)
    max_depth_scanned: int = 0
    folders_scanned_count: int = 0
    total_valid_xml: int = 0
    total_valid_xml_gz: int = 0
    all_valid_remote_paths: list[str] = field(default_factory=list)
    sample_valid_paths: list[str] = field(default_factory=list)

    def all_valid_local_names(self) -> set[str]:
        out: set[str] = set()
        for folder in self.folders:
            out.update(folder.all_valid_local_names())
        return out


def _is_dir(sftp, path: str) -> bool:
    import stat
    try:
        return stat.S_ISDIR(sftp.stat(path).st_mode)
    except OSError:
        return False


def _list_dirs(sftp, path: str) -> list[str]:
    try:
        attrs = sftp.listdir_attr(path)
        return sorted(
            a.filename for a in attrs
            if not a.filename.startswith(".") and _is_dir(sftp, f"{path}/{a.filename}")
        )
    except OSError:
        return []


def _list_files(sftp, path: str) -> list[str]:
    try:
        attrs = sftp.listdir_attr(path)
        return sorted(
            a.filename for a in attrs
            if not a.filename.startswith(".") and not _is_dir(sftp, f"{path}/{a.filename}")
        )
    except OSError:
        return []


def _scan_folder(sftp, issuer: str, folder_path: str, depth: int, parent_path: str) -> FolderScan:
    scan = FolderScan(
        folder_path=folder_path,
        depth=depth,
        parent_path=parent_path,
    )
    try:
        files = _list_files(sftp, folder_path)
    except Exception as exc:
        scan.status = "SCAN_ERROR"
        scan.message = str(exc)
        return scan

    scan.total_files = len(files)
    for filename in files:
        category = classify_sftp_filename(issuer, filename)
        remote_file = f"{folder_path}/{filename}"
        if category == "valid_xml":
            scan.valid_xml_count += 1
            scan.valid_xml_names.append(filename)
            scan.valid_remote_paths.append(remote_file)
        elif category == "valid_gz":
            scan.valid_xml_gz_count += 1
            scan.valid_xml_gz_names.append(filename)
            scan.valid_remote_paths.append(remote_file)
        elif category in SKIP_COUNT_KEYS:
            col = SKIP_COUNT_KEYS[category]
            setattr(scan, col, getattr(scan, col) + 1)

    valid_total = scan.valid_xml_count + scan.valid_xml_gz_count
    if valid_total > 0:
        scan.status = "HAS_VALID_XML"
        scan.message = (
            f"Found {scan.valid_xml_count} .xml and {scan.valid_xml_gz_count} .xml.gz file(s)"
        )
    elif scan.total_files == 0 and depth > 0:
        scan.status = "EMPTY_FOLDER"
        scan.message = "Folder contains no files"
    elif scan.total_files > 0:
        scan.status = "NO_VALID_XML"
        scan.message = "Files present but no valid from_{issuer}_GA_834_INDV_*.xml(.gz)"

    return scan


def walk_partition_month(
    sftp,
    month_path: str,
    issuer: str,
) -> PartitionWalkResult:
    """
    Recursively walk every subfolder under month_path (unlimited depth).

    Starts at depth 0 (the month folder itself).
    """
    result = PartitionWalkResult(month_path=month_path)

    def _walk(path: str, depth: int, parent: str) -> None:
        scan = _scan_folder(sftp, issuer, path, depth, parent)
        result.folders.append(scan)
        result.folders_scanned_count += 1
        result.max_depth_scanned = max(result.max_depth_scanned, depth)
        result.total_valid_xml += scan.valid_xml_count
        result.total_valid_xml_gz += scan.valid_xml_gz_count
        result.all_valid_remote_paths.extend(scan.valid_remote_paths)

        for subdir in _list_dirs(sftp, path):
            _walk(f"{path}/{subdir}", depth + 1, path)

    try:
        _walk(month_path, 0, "")
    except Exception as exc:
        logger.error("Recursive walk failed at %s: %s", month_path, exc)
        result.folders.append(FolderScan(
            folder_path=month_path,
            depth=0,
            parent_path="",
            status="WALK_ERROR",
            message=str(exc),
        ))

    result.sample_valid_paths = result.all_valid_remote_paths[:10]
    return result
