"""Filename timestamp parsing for 834 source files."""

from __future__ import annotations

import re
from pathlib import Path

_FILENAME_TS_AFTER_INDV = re.compile(
    r"(?:INDV|INVD)_(\d{4})(\d{2})(\d{2})\d{6}\.xml$",
    re.IGNORECASE,
)
_FILENAME_TS_FALLBACK = re.compile(r"_(\d{4})(\d{2})(\d{2})\d{6}\.xml$", re.IGNORECASE)


def parse_filename_year_month(filename: str) -> tuple[int | None, int | None]:
    """
    Parse YYYYMMDD from standard 834 filenames.

    Returns (year, month) as integers, or (None, None) when not parseable.
    """
    name = Path(str(filename)).name
    match = _FILENAME_TS_AFTER_INDV.search(name) or _FILENAME_TS_FALLBACK.search(name)
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))
