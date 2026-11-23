"""
Outbound TO-file selection.

Evidence used (ingestion/sftp_file_classifier.py, inbound — unchanged):
  * a remote name whose lowercase form starts with "to_" is a TO file
    (inbound skips it as category "to");
  * FROM files match "from_{issuer}_GA_834_INDV_*.xml[.gz|.xz]";
  * .xml / .xml.gz / .xml.xz are the accepted 834 XML payload extensions;
  * names containing edi / tracking / report / summary / log / .txt / .dtl are not 834 XML.

The repository contains no example TO filename beyond the "to_" prefix, so the
outbound rule is: "to_" prefix + the same exclusions + the same XML extensions.
It does NOT assume a "to_{issuer}_GA_834_INDV_" infix. Use --sftp-audit to list the
real TO filenames before tightening TO_FILE_PREFIX / adding a pattern.
"""

from __future__ import annotations

TO_FILE_PREFIX = "to_"
FROM_FILE_PREFIX = "from_"

VALID_CLASSES = frozenset({"valid_xml", "valid_gz", "valid_xz"})


def _non_834_reason(lower: str) -> str | None:
    """Same exclusion order as the inbound classifier, minus its to_ skip."""
    if "log" in lower and (lower.endswith(".txt") or lower.endswith(".txt.xz") or lower == "log.txt"):
        return "log"
    if lower.endswith(".txt") or lower.endswith(".txt.gz") or lower.endswith(".txt.xz"):
        return "other"
    if ".edi" in lower:
        return "edi"
    if ".dtl" in lower:
        return "other"
    if "tracking" in lower:
        return "tracking"
    if "report" in lower:
        return "report"
    if "summary" in lower:
        return "summary"
    if "log" in lower:
        return "log"
    return None


def classify_outbound_filename(filename: str) -> str:
    """
    Returns: valid_xml, valid_gz, valid_xz, from, edi, tracking, report, summary, log, other.
    """
    lower = filename.lower()
    if lower.startswith(FROM_FILE_PREFIX):
        return "from"
    if not lower.startswith(TO_FILE_PREFIX):
        return "other"

    reason = _non_834_reason(lower)
    if reason:
        return reason

    if lower.endswith(".xml.xz"):
        return "valid_xz"
    if lower.endswith(".xml.gz"):
        return "valid_gz"
    if lower.endswith(".xml"):
        return "valid_xml"
    return "other"


def is_outbound_to_file(filename: str) -> bool:
    return classify_outbound_filename(filename) in VALID_CLASSES


def is_outbound_local_xml(filename: str) -> bool:
    """Local (already decompressed) file accepted by outbound discovery."""
    return classify_outbound_filename(filename) == "valid_xml"
