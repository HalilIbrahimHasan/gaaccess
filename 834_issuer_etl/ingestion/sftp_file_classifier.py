"""Classify remote SFTP filenames for ingestion and audit."""

from __future__ import annotations

import fnmatch


def classify_sftp_filename(issuer: str, filename: str) -> str:
    """
    Classify a remote filename.

    Returns one of: valid, edi, tracking, report, summary, log, to, other
    """
    lower = filename.lower()
    if lower.startswith("to_"):
        return "to"
    if "tracking" in lower:
        return "tracking"
    if "edi" in lower:
        return "edi"
    if "report" in lower:
        return "report"
    if "summary" in lower:
        return "summary"
    if "log" in lower:
        return "log"

    pattern = f"from_{issuer}_GA_834_INDV_*.xml.gz"
    if fnmatch.fnmatch(filename, pattern):
        return "valid"
    return "other"


def is_valid_834_gz(issuer: str, filename: str) -> bool:
    return classify_sftp_filename(issuer, filename) == "valid"


def local_xml_name_from_gz(filename: str) -> str:
    return filename[:-3] if filename.endswith(".gz") else filename
