"""
Outbound TO-file selection.

Standard individual-market TO file:

    to_<issuer>_<exchange>_834_INDV_<timestamp>.xml[.gz|.xz]

  * issuer    : 5-digit HIOS issuer id (captured, never hard-coded)
  * exchange  : must be in the configured exchange codes (OUTBOUND_EXCHANGE_CODES)
  * timestamp : digits (captured); filename year/month = first 6 digits when valid

Exclusions (edi / tracking / report / summary / log / .txt / .dtl) follow the
inbound classifier (ingestion/sftp_file_classifier.py, unchanged). A "to_" XML file
that does not match the standard pattern is classified "to_nonstandard": it is
reported, never loaded as a standard individual 834 file.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from outbound_automation.config import resolve_exchange_codes

TO_FILE_PREFIX = "to_"
FROM_FILE_PREFIX = "from_"

VALID_CLASSES = frozenset({"valid_xml", "valid_gz", "valid_xz"})
REPORTED_CLASSES = frozenset({"to_nonstandard"})
ALL_CLASSES = (
    "valid_xml", "valid_gz", "valid_xz", "to_nonstandard", "from",
    "edi", "tracking", "report", "summary", "log", "other",
)

_STANDARD_TO_RE = re.compile(
    r"^to_(?P<issuer>\d{5})_(?P<exchange>[A-Za-z]{2,})_834_INDV_(?P<timestamp>\d{8,20})"
    r"\.xml(?:\.gz|\.xz)?$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ToFileName:
    issuer: str
    exchange: str
    timestamp: str
    file_year: int | None
    file_month: int | None


def parse_to_filename(
    filename: str, *, exchange_codes: frozenset[str] | None = None,
) -> ToFileName | None:
    """Parse a standard TO filename; None when the name is not standard."""
    match = _STANDARD_TO_RE.match(filename.strip())
    if not match:
        return None
    exchange = match.group("exchange").upper()
    allowed = exchange_codes if exchange_codes is not None else resolve_exchange_codes()
    if exchange not in allowed:
        return None
    ts = match.group("timestamp")
    year, month = int(ts[:4]), int(ts[4:6])
    valid_period = 1 <= month <= 12
    return ToFileName(
        issuer=match.group("issuer"),
        exchange=exchange,
        timestamp=ts,
        file_year=year if valid_period else None,
        file_month=month if valid_period else None,
    )


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


def classify_outbound_filename(
    filename: str, *, exchange_codes: frozenset[str] | None = None,
) -> str:
    """Returns one of ALL_CLASSES."""
    lower = filename.lower()
    if lower.startswith(FROM_FILE_PREFIX):
        return "from"
    if not lower.startswith(TO_FILE_PREFIX):
        return "other"

    reason = _non_834_reason(lower)
    if reason:
        return reason

    if not (lower.endswith(".xml") or lower.endswith(".xml.gz") or lower.endswith(".xml.xz")):
        return "other"
    if parse_to_filename(filename, exchange_codes=exchange_codes) is None:
        return "to_nonstandard"
    if lower.endswith(".xml.xz"):
        return "valid_xz"
    if lower.endswith(".xml.gz"):
        return "valid_gz"
    return "valid_xml"


def is_outbound_to_file(filename: str, *, exchange_codes: frozenset[str] | None = None) -> bool:
    return classify_outbound_filename(filename, exchange_codes=exchange_codes) in VALID_CLASSES


def is_outbound_local_xml(filename: str, *, exchange_codes: frozenset[str] | None = None) -> bool:
    """Local (already decompressed) file accepted by outbound discovery."""
    return classify_outbound_filename(filename, exchange_codes=exchange_codes) == "valid_xml"
