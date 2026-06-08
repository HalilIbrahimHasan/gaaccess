"""
Partition discovery for issuer/year/month folder structure.

``discover_partitions()`` is the single source of truth for dynamic ETL
execution. It recursively walks ``source_data/{issuer}/{year}/{month}/`` and
returns one ``SourcePartition`` per folder that contains XML files.
"""

from dataclasses import dataclass, field
from pathlib import Path

from config import ASSETS_DIR, SOURCE_DATA_DIR
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class SourcePartition:
    """
    A single issuer/year/month data partition discovered under ``source_data``.

    Bundles input paths, XML file list, and resolved output path so every
    pipeline stage processes data at the correct granularity.
    """

    issuer_id: str
    year: str
    month: str
    input_path: Path
    xml_files: list[Path] = field(default_factory=list)
    output_path: Path = field(default_factory=Path)

    @property
    def source_period(self) -> str:
        """Return period label in ``YYYY-MM`` format."""
        return f"{self.year}-{self.month}"

    @property
    def period_key(self) -> str:
        """Return filesystem-safe key ``{issuer_id}_{year}_{month}``."""
        return f"{self.issuer_id}_{self.year}_{self.month}"

    @property
    def file_count(self) -> int:
        """Number of XML files in this partition."""
        return len(self.xml_files)

    def __post_init__(self) -> None:
        """Resolve default output path when not supplied."""
        if not self.output_path or str(self.output_path) == ".":
            self.output_path = (
                ASSETS_DIR / self.issuer_id / self.year / self.month
            )


# Backward-compatible alias used across pipeline modules
Partition = SourcePartition


def discover_partitions(
    source_root: Path | None = None,
    issuer_id: str | None = None,
    year: str | None = None,
    month: str | None = None,
) -> list[SourcePartition]:
    """
    Recursively discover all issuer/year/month folders containing XML files.

    This function is the source of truth for dynamic ETL execution. A valid
    partition requires:

    * ``issuer_id`` folder is numeric
    * ``year`` folder is exactly 4 digits
    * ``month`` folder is exactly 2 digits (01-12)
    * month folder contains at least one ``*.xml`` file

    Args:
        source_root: Root input directory; defaults to ``SOURCE_DATA_DIR``.
        issuer_id: Optional filter — process only this issuer.
        year: Optional filter — process only this year (use with issuer).
        month: Optional filter — process only this month (use with issuer+year).

    Returns:
        Sorted list of ``SourcePartition`` instances.
    """
    root = source_root or SOURCE_DATA_DIR
    if not root.exists():
        logger.warning("Source data directory does not exist: %s", root)
        return []

    partitions: list[SourcePartition] = []

    for issuer_dir in sorted(root.iterdir(), key=lambda p: p.name):
        if not issuer_dir.is_dir() or not issuer_dir.name.isdigit():
            continue
        if issuer_id and issuer_dir.name != issuer_id:
            continue

        for year_dir in sorted(issuer_dir.iterdir(), key=lambda p: p.name):
            if not year_dir.is_dir():
                continue
            if not year_dir.name.isdigit() or len(year_dir.name) != 4:
                continue
            if year and year_dir.name != year:
                continue

            for month_dir in sorted(year_dir.iterdir(), key=lambda p: p.name):
                if not month_dir.is_dir():
                    continue
                if not month_dir.name.isdigit() or len(month_dir.name) != 2:
                    continue
                if month and month_dir.name != month.zfill(2):
                    continue
                if not _is_valid_month(month_dir.name):
                    continue

                xml_files = sorted(month_dir.glob("*.xml"))
                if not xml_files:
                    continue

                partitions.append(
                    SourcePartition(
                        issuer_id=issuer_dir.name,
                        year=year_dir.name,
                        month=month_dir.name,
                        input_path=month_dir,
                        xml_files=xml_files,
                        output_path=ASSETS_DIR / issuer_dir.name / year_dir.name / month_dir.name,
                    )
                )

    logger.info("Found %d source partition(s).", len(partitions))
    return partitions


def ensure_partition_asset_dirs(partition: SourcePartition) -> dict[str, Path]:
    """
    Create monthly output directories for a partition.

    Args:
        partition: Target issuer/year/month partition.

    Returns:
        Dict of logical output names to directory paths.
    """
    base = partition.output_path
    dirs = {
        "base": base,
        "excel": base / "excel",
        "cleaned_xml": base / "cleaned_xml",
        "sqlite": base / "sqlite",
        "dashboards": base / "dashboards",
        "validation_reports": base / "validation_reports",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def ensure_rollup_asset_dirs(issuer_id: str) -> dict[str, Path]:
    """
    Create issuer-level rollup output directories.

    Rollups aggregate all monthly partitions for one issuer into combined
    outputs under ``assets/{issuer_id}/rollups/``.

    Args:
        issuer_id: Issuer identifier.

    Returns:
        Dict of logical rollup output names to directory paths.
    """
    base = ASSETS_DIR / issuer_id / "rollups"
    dirs = {
        "base": base,
        "excel": base / "excel",
        "sqlite": base / "sqlite",
        "dashboards": base / "dashboards",
        "validation_reports": base / "validation_reports",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def monthly_output_stem(partition: SourcePartition) -> str:
    """Return filename stem ``{issuer_id}_{year}_{month}`` for monthly outputs."""
    return partition.period_key


def rollup_output_stem(issuer_id: str) -> str:
    """Return filename stem ``{issuer_id}_all_periods`` for rollup outputs."""
    return f"{issuer_id}_all_periods"


def _is_valid_month(name: str) -> bool:
    """Return True when folder name is a valid two-digit month (01-12)."""
    return len(name) == 2 and name.isdigit() and 1 <= int(name) <= 12
