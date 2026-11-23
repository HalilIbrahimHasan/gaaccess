"""
Outbound business mappings, isolated from parser traversal.

* insurance type: configuration-driven (insurance_type_map_v1.json, or the file
  named by OUTBOUND_INSURANCE_TYPE_MAP_PATH). Unknown codes map to None + warning.
* outbound transaction type: INS03 event type (+ additional maintenance reason
  for 024), per the companion guide INS03 code list.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from outbound_automation.constants import ENV_INSURANCE_TYPE_MAP_PATH, INSURANCE_TYPE_MAP_FILE

W_UNKNOWN_INSURANCE_TYPE = "UNKNOWN_INSURANCE_TYPE"
W_MISSING_INSURANCE_TYPE_CODE = "MISSING_INSURANCE_TYPE_CODE"
W_UNKNOWN_TRANSACTION_TYPE = "UNKNOWN_TRANSACTION_TYPE"

# INS03 -> transaction type (companion guide: 001 Change, 021 Addition,
# 024 Cancellation or Termination, 025 Reinstatement).
TRANSACTION_TYPE_BY_EVENT_CODE: dict[str, str] = {
    "021": "ADD",
    "001": "CHANGE",
    "025": "REINSTATE",
}
CANCEL_OR_TERM_EVENT_CODE = "024"
CANCEL_OR_TERM_BY_ADDITIONAL_REASON: dict[str, str] = {
    "CANCEL": "CANCEL",
    "TERM": "TERM",
}
CANCEL_OR_TERM_FALLBACK = "CANCEL_OR_TERM"


def load_insurance_type_map(path: Path | None = None) -> dict[str, str]:
    if path is None:
        override = os.getenv(ENV_INSURANCE_TYPE_MAP_PATH, "").strip().strip('"').strip("'")
        path = Path(override) if override else INSURANCE_TYPE_MAP_FILE
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return {str(k).strip().upper(): str(v) for k, v in data.get("mappings", {}).items()}


def map_insurance_type(
    code: str | None, mapping: dict[str, str],
) -> tuple[str | None, str | None]:
    """Return (display value, warning code)."""
    if code is None or not code.strip():
        return None, W_MISSING_INSURANCE_TYPE_CODE
    display = mapping.get(code.strip().upper())
    if display is None:
        return None, W_UNKNOWN_INSURANCE_TYPE
    return display, None


def classify_outbound_transaction(
    event_type_code: str | None, additional_maint_reason: str | None,
) -> tuple[str | None, str | None]:
    """Return (outbound_transaction_type, warning code)."""
    code = (event_type_code or "").strip()
    if code == CANCEL_OR_TERM_EVENT_CODE:
        reason = (additional_maint_reason or "").strip().upper()
        return CANCEL_OR_TERM_BY_ADDITIONAL_REASON.get(reason, CANCEL_OR_TERM_FALLBACK), None
    mapped = TRANSACTION_TYPE_BY_EVENT_CODE.get(code)
    if mapped is None:
        return None, W_UNKNOWN_TRANSACTION_TYPE
    return mapped, None
