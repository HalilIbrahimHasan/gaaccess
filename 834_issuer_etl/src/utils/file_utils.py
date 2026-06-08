"""
File and path utilities for the 834 Issuer ETL framework.

Provides filename parsing helpers used during XML parsing and cleaning.
Partition discovery and output directory creation live in ``partition.py``.
"""

import re
from pathlib import Path

# Pattern to extract issuer ID from filenames like from_64357_GA_834_...
_ISSUER_FILENAME_PATTERN = re.compile(r"from_(\d+)_", re.IGNORECASE)


def parse_issuer_from_filename(filename: str) -> str | None:
    """
    Extract issuer ID from a standard 834 filename when folder name is absent.

    Args:
        filename: Base name or full path of an XML file.

    Returns:
        Issuer ID string or ``None`` if the pattern does not match.
    """
    match = _ISSUER_FILENAME_PATTERN.search(Path(filename).name)
    return match.group(1) if match else None


def parse_file_date_from_filename(filename: str) -> str | None:
    """
    Extract a YYYYMMDD date token from filenames like ``..._20260204071545.xml``.

    Args:
        filename: XML file name.

    Returns:
        Eight-digit date string or ``None``.
    """
    match = re.search(r"_(\d{8})\d{6}\.xml$", Path(filename).name, re.IGNORECASE)
    return match.group(1) if match else None
