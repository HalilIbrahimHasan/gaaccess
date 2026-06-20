"""
Normalize enrollment status values to canonical lifecycle states.
"""

from __future__ import annotations

STATUS_MAP: dict[str, str] = {
    "CONFIRM": "ENROLLED",
    "CONFIRMED": "ENROLLED",
    "CONFIRMATION": "ENROLLED",
    "REINSTATE": "ENROLLED",
    "REINSTATED": "ENROLLED",
    "ACTIVE": "ENROLLED",
    "ENROLLED": "ENROLLED",
    "EFFECTUATED": "ENROLLED",
    "EC": "ENROLLED",
    "CANCEL": "CANCELLED",
    "CANCELLED": "CANCELLED",
    "CANCELED": "CANCELLED",
    "CANCELLATION": "CANCELLED",
    "TERM": "TERMINATED",
    "TERMINATED": "TERMINATED",
    "TERMINATION": "TERMINATED",
    "PENDING": "PENDING",
    "PEND": "PENDING",
    "UNKNOWN": "UNKNOWN",
}


def normalize_status(raw: str | None) -> str:
    if raw is None:
        return "UNKNOWN"
    key = str(raw).strip().upper()
    if not key:
        return "UNKNOWN"
    if key in STATUS_MAP:
        return STATUS_MAP[key]
    for token, canonical in STATUS_MAP.items():
        if token in key:
            return canonical
    return "UNKNOWN"


def normalize_insurance_type(raw: str | None) -> str:
    if raw is None:
        return "HEALTH"
    key = str(raw).strip().upper()
    if key in ("HLT", "HEALTH", "H", "MEDICAL", "MED"):
        return "HEALTH"
    if key in ("DEN", "DENTAL", "D"):
        return "DENTAL"
    if key in ("VIS", "VISION", "V"):
        return "VISION"
    return key or "HEALTH"
