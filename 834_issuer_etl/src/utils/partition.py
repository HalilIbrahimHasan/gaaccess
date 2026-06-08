"""
Partition discovery for issuer/year/month folder structure.

Fully folder-driven: every numeric issuer / 4-digit year / 2-digit month folder
with XML files under ``source_data/`` is discovered automatically. No config
issuer list or CLI filters required.
"""

from dataclasses import dataclass, field
from pathlib import Path

import config
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
                config.ASSETS_DIR / self.issuer_id / self.year / self.month
            )


Partition = SourcePartition


def discover_partitions(source_root: Path | None = None) -> list[SourcePartition]:
    """
    Discover every valid issuer/year/month partition under ``source_data``.

    Walks ``{issuer_id}/{year}/{month}/*.xml`` dynamically — new issuer folders
    are picked up automatically with no config or code changes.

    Args:
        source_root: Override source directory; defaults to ``SOURCE_DATA_DIR``.

    Returns:
        Sorted list of all ``SourcePartition`` instances with XML files.
    """
    root = source_root or config.SOURCE_DATA_DIR
    partitions: list[SourcePartition] = []

    if not root.exists():
        logger.warning("SOURCE_ROOT does not exist: %s", root)
        logger.info("Discovered partitions:")
        return partitions

    for issuer_dir in sorted(root.iterdir(), key=lambda p: p.name):
        if not issuer_dir.is_dir() or not issuer_dir.name.isdigit():
            continue

        for year_dir in sorted(issuer_dir.iterdir(), key=lambda p: p.name):
            if not year_dir.is_dir():
                continue
            if not year_dir.name.isdigit() or len(year_dir.name) != 4:
                continue

            for month_dir in sorted(year_dir.iterdir(), key=lambda p: p.name):
                if not month_dir.is_dir():
                    continue
                if not month_dir.name.isdigit() or len(month_dir.name) != 2:
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
                        output_path=(
                            config.ASSETS_DIR
                            / issuer_dir.name
                            / year_dir.name
                            / month_dir.name
                        ),
                    )
                )

    logger.info("Discovered partitions:")
    for p in partitions:
        logger.info(
            "  issuer=%s, year=%s, month=%s, files=%d",
            p.issuer_id,
            p.year,
            p.month,
            p.file_count,
        )
    if not partitions:
        logger.info("  (none — add source_data/{issuer}/{year}/{month}/*.xml)")

    return partitions


def ensure_partition_asset_dirs(partition: SourcePartition) -> dict[str, Path]:
    """Create monthly output directories for a partition."""
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
    """Create issuer-level rollup output directories."""
    base = config.ASSETS_DIR / issuer_id / "rollups"
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
    """Return filename stem ``{issuer_id}_{year}_{month}``."""
    return partition.period_key


def rollup_output_stem(issuer_id: str) -> str:
    """Return filename stem ``{issuer_id}_all_periods``."""
    return f"{issuer_id}_all_periods"
