"""Classify remote SFTP filenames for ingestion and audit."""

from __future__ import annotations

import fnmatch


def _is_edi_skip(filename: str) -> bool:
    lower = filename.lower()
    return lower.endswith((".edi", ".edi.gz", ".edi.bad", ".edi.good"))


def classify_sftp_filename(issuer: str, filename: str) -> str:
    """
    Classify a remote filename.

    Returns one of:
        valid_xml, valid_gz, edi, tracking, report, summary, log, to, other
    """
    lower = filename.lower()
    if lower.startswith("to_"):
        return "to"
    if _is_edi_skip(filename):
        return "edi"
    if "tracking" in lower:
        return "tracking"
    if "report" in lower:
        return "report"
    if "summary" in lower:
        return "summary"
    if "log" in lower:
        return "log"

    gz_pattern = f"from_{issuer}_GA_834_INDV_*.xml.gz"
    xml_pattern = f"from_{issuer}_GA_834_INDV_*.xml"
    if fnmatch.fnmatch(filename, gz_pattern):
        return "valid_gz"
    if fnmatch.fnmatch(filename, xml_pattern):
        return "valid_xml"
    return "other"


def is_valid_834_file(issuer: str, filename: str) -> bool:
    return classify_sftp_filename(issuer, filename) in ("valid_xml", "valid_gz")


# Backward-compatible alias
def is_valid_834_gz(issuer: str, filename: str) -> bool:
    return is_valid_834_file(issuer, filename)


def local_xml_name_from_remote(filename: str) -> str:
    """Map remote .xml or .xml.gz name to flat local .xml filename."""
    if filename.lower().endswith(".xml.gz"):
        return filename[:-3]
    return filename


# Backward-compatible alias
def local_xml_name_from_gz(filename: str) -> str:
    return local_xml_name_from_remote(filename)
