"""
Partition discovery for issuer/year/month folder structure.

Scans ``source_data/{issuer_id}/{year}/{month}/`` and returns ``Partition``
objects that carry both input paths and matching output paths so every
pipeline stage can process data at the correct granularity.
"""

from dataclasses import dataclass, field
from pathlib import Path

from config import ASSETS_DIR, SOURCE_DATA_DIR
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class Partition:
    """
    A single issuer/year/month data partition.

    Bundles discovery metadata and resolved paths so extract, transform,
    validate, and load stages share one partition contract.
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

    def __post_init__(self) -> None:
        """Resolve default output path when not supplied."""
        if not self.output_path or str(self.output_path) == ".":
            self.output_path = ASSETS_DIR / self.issuer_id / self.year / self.month


def discover_partitions(
    source_root: Path | None = None,
    issuer_id: str | None = None,
    year: str | None = None,
    month: str | None = None,
) -> list[Partition]:
    """
    Discover all issuer/year/month partitions under ``source_data``.

    Walks ``source_root/{issuer_id}/{year}/{month}/`` and collects XML files
    in each month folder. Optional filters narrow discovery for CLI usage.

    Args:
        source_root: Root input directory; defaults to ``SOURCE_DATA_DIR``.
        issuer_id: Process only this issuer when set.
        year: Process only this year when set (requires issuer context).
        month: Process only this month when set (requires issuer + year).

    Returns:
        Sorted list of ``Partition`` instances with populated ``xml_files``.
    """
    root = source_root or SOURCE_DATA_DIR
    if not root.exists():
        logger.warning("Source data directory does not exist: %s", root)
        return []

    issuer_dirs = _list_issuer_dirs(root, issuer_id)
    partitions: list[Partition] = []

    for issuer_dir in issuer_dirs:
        year_dirs = _list_year_dirs(issuer_dir, year)
        for year_dir in year_dirs:
            month_dirs = _list_month_dirs(year_dir, month)
            for month_dir in month_dirs:
                xml_files = sorted(month_dir.glob("*.xml"))
                if not xml_files:
                    logger.debug(
                        "Skipping empty partition (no XML): %s", month_dir
                    )
                    continue
                partition = Partition(
                    issuer_id=issuer_dir.name,
                    year=year_dir.name,
                    month=month_dir.name,
                    input_path=month_dir,
                    xml_files=xml_files,
                    output_path=ASSETS_DIR / issuer_dir.name / year_dir.name / month_dir.name,
                )
                partitions.append(partition)

    partitions.sort(key=lambda p: (p.issuer_id, p.year, p.month))
    logger.info(
        "Discovered %d partition(s)%s",
        len(partitions),
        _filter_label(issuer_id, year, month),
    )
    return partitions


def ensure_partition_asset_dirs(partition: Partition) -> dict[str, Path]:
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
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def monthly_output_stem(partition: Partition) -> str:
    """Return filename stem ``{issuer_id}_{year}_{month}`` for monthly outputs."""
    return partition.period_key


def rollup_output_stem(issuer_id: str) -> str:
    """Return filename stem ``{issuer_id}_all_periods`` for rollup outputs."""
    return f"{issuer_id}_all_periods"


def _list_issuer_dirs(root: Path, issuer_id: str | None) -> list[Path]:
    """Return issuer directories, optionally filtered to one issuer."""
    if issuer_id:
        path = root / issuer_id
        return [path] if path.is_dir() else []
    return sorted(
        d for d in root.iterdir() if d.is_dir() and d.name.isdigit()
    )


def _list_year_dirs(issuer_dir: Path, year: str | None) -> list[Path]:
    """Return year directories under an issuer, optionally filtered."""
    if year:
        path = issuer_dir / year
        return [path] if path.is_dir() and _is_year(path.name) else []
    return sorted(
        d for d in issuer_dir.iterdir() if d.is_dir() and _is_year(d.name)
    )


def _list_month_dirs(year_dir: Path, month: str | None) -> list[Path]:
    """Return month directories under a year, optionally filtered."""
    if month:
        normalized = month.zfill(2)
        path = year_dir / normalized
        return [path] if path.is_dir() and _is_month(path.name) else []
    return sorted(
        d for d in year_dir.iterdir() if d.is_dir() and _is_month(d.name)
    )


def _is_year(name: str) -> bool:
    """Return True when folder name is a four-digit year."""
    return len(name) == 4 and name.isdigit()


def _is_month(name: str) -> bool:
    """Return True when folder name is a valid month (01-12)."""
    return len(name) == 2 and name.isdigit() and 1 <= int(name) <= 12


def _filter_label(
    issuer_id: str | None, year: str | None, month: str | None
) -> str:
    """Build a human-readable filter description for logging."""
    parts = []
    if issuer_id:
        parts.append(f" issuer={issuer_id}")
    if year:
        parts.append(f" year={year}")
    if month:
        parts.append(f" month={month.zfill(2)}")
    return f" ({','.join(parts).strip()})" if parts else ""
