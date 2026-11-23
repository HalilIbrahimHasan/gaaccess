"""
Application-level parent/child integrity for one file.

dbo.outbound_automation_monthly_premium has no SQL FOREIGN KEY, so every premium
row must be proven to belong to a parent row of the same file before the file
transaction starts.
"""

from __future__ import annotations

from typing import Any


class ParentChildIntegrityError(ValueError):
    """Parent/child rows of a file are inconsistent; the file must not be loaded."""


def validate_parent_child(
    file_hash: str,
    rows: list[dict[str, Any]],
    premium_rows: list[dict[str, Any]],
) -> None:
    if not rows:
        raise ParentChildIntegrityError("no parent rows")

    parents: dict[int, dict[str, Any]] = {}
    for row in rows:
        if row.get("file_hash") != file_hash:
            raise ParentChildIntegrityError("parent row file_hash differs from file")
        rn = row.get("row_number_in_file")
        if not isinstance(rn, int) or rn in parents:
            raise ParentChildIntegrityError(f"parent row_number_in_file invalid or duplicated: {rn}")
        parents[rn] = row
    if sorted(parents) != list(range(1, len(parents) + 1)):
        raise ParentChildIntegrityError("parent row_number_in_file is not contiguous 1..N")

    seen: set[tuple[int, int]] = set()
    for child in premium_rows:
        if child.get("file_hash") != file_hash:
            raise ParentChildIntegrityError("premium row file_hash differs from file")
        rn = child.get("row_number_in_file")
        parent = parents.get(rn)
        if parent is None:
            raise ParentChildIntegrityError(f"premium row references missing parent row {rn}")
        key = (rn, child.get("premium_seq"))
        if key in seen:
            raise ParentChildIntegrityError(f"duplicate premium_seq for parent row {rn}")
        seen.add(key)
        for col in ("policy_id", "member_id", "issuer", "load_run_id"):
            if child.get(col) != parent.get(col):
                raise ParentChildIntegrityError(
                    f"premium row {col} differs from parent row {rn}"
                )
