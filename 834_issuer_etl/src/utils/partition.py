"""
Partition discovery for issuer/year/month folder structure.

Discovery runs in two steps:
1. ``discover_all_partitions()`` — find every valid partition (no CLI filters).
2. ``filter_partitions()`` — apply optional --issuer / --year / --month filters.

``log_source_tree_diagnostic()`` prints the full folder tree before filtering.
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


def log_source_tree_diagnostic(source_root: Path | None = None) -> None:
    """
    Print a full diagnostic walk of ``source_data`` before any CLI filtering.

    Shows issuer/year/month folders, XML counts, and warnings for XML files
    placed at the wrong directory level.
    """
    root = source_root or config.SOURCE_DATA_DIR
    logger.info("=" * 60)
    logger.info("SOURCE TREE DIAGNOSTIC (before filters)")
    logger.info("=" * 60)
    logger.info("SOURCE_ROOT path          : %s", root)
    logger.info("SOURCE_ROOT exists      : %s", root.exists())

    if not root.exists():
        logger.info("Issuer folders          : (source root missing)")
        logger.info("=" * 60)
        return

    issuer_dirs = sorted(
        (p for p in root.iterdir() if p.is_dir() and not p.name.startswith(".")),
        key=lambda p: p.name,
    )
    issuer_names = [p.name for p in issuer_dirs]
    logger.info("Issuer folders found    : %s", issuer_names if issuer_names else "(none)")

    for issuer_dir in issuer_dirs:
        if not issuer_dir.name.isdigit():
            loose_xml = sorted(issuer_dir.glob("*.xml"))
            logger.info(
                "  issuer=%s (SKIPPED — not numeric)%s",
                issuer_dir.name,
                f" — {len(loose_xml)} XML at issuer level" if loose_xml else "",
            )
            continue

        loose_xml = sorted(issuer_dir.glob("*.xml"))
        if loose_xml:
            logger.warning(
                "  issuer=%s — %d XML file(s) at WRONG level "
                "(expected source_data/%s/{year}/{month}/, not directly under issuer)",
                issuer_dir.name,
                len(loose_xml),
                issuer_dir.name,
            )

        year_dirs = sorted(
            (p for p in issuer_dir.iterdir() if p.is_dir() and not p.name.startswith(".")),
            key=lambda p: p.name,
        )
        if not year_dirs:
            logger.info("  issuer=%s — no year subfolders", issuer_dir.name)
            continue

        for year_dir in year_dirs:
            if not _is_valid_year(year_dir.name):
                loose_xml = sorted(year_dir.glob("*.xml"))
                logger.info(
                    "    year=%s (SKIPPED — not 4-digit year)%s",
                    year_dir.name,
                    f" — {len(loose_xml)} XML at year level" if loose_xml else "",
                )
                continue

            month_dirs = sorted(
                (p for p in year_dir.iterdir() if p.is_dir() and not p.name.startswith(".")),
                key=lambda p: p.name,
            )
            if not month_dirs:
                logger.info("    issuer=%s year=%s — no month subfolders", issuer_dir.name, year_dir.name)
                continue

            for month_dir in month_dirs:
                normalized_month = _normalize_month(month_dir.name)
                xml_files = sorted(month_dir.glob("*.xml"))
                if normalized_month is None:
                    logger.info(
                        "      month=%s (SKIPPED — invalid month name) xml=%d",
                        month_dir.name,
                        len(xml_files),
                    )
                else:
                    status = "VALID" if xml_files else "EMPTY (no XML)"
                    logger.info(
                        "      issuer=%s year=%s month=%s [%s] xml=%d path=%s",
                        issuer_dir.name,
                        year_dir.name,
                        normalized_month,
                        status,
                        len(xml_files),
                        month_dir,
                    )

    logger.info("=" * 60)


def discover_all_partitions(source_root: Path | None = None) -> list[SourcePartition]:
    """
    Discover every valid issuer/year/month partition — no CLI filters applied.

    A valid partition requires:
    * numeric issuer_id folder
    * 4-digit year folder
    * month folder (01-12, accepts 1 or 2 digit names normalized to 02)
    * at least one ``*.xml`` in the month folder

    Returns:
        Sorted list of all ``SourcePartition`` instances under ``source_root``.
    """
    root = source_root or config.SOURCE_DATA_DIR
    if not root.exists():
        logger.warning("Source data directory does not exist: %s", root)
        return []

    partitions: list[SourcePartition] = []

    for issuer_dir in sorted(
        (p for p in root.iterdir() if p.is_dir() and p.name.isdigit()),
        key=lambda p: p.name,
    ):
        for year_dir in sorted(
            (p for p in issuer_dir.iterdir() if p.is_dir() and _is_valid_year(p.name)),
            key=lambda p: p.name,
        ):
            for month_dir in sorted(
                (p for p in year_dir.iterdir() if p.is_dir()),
                key=lambda p: p.name,
            ):
                month = _normalize_month(month_dir.name)
                if month is None:
                    continue
                xml_files = sorted(month_dir.glob("*.xml"))
                if not xml_files:
                    continue
                partitions.append(
                    SourcePartition(
                        issuer_id=issuer_dir.name,
                        year=year_dir.name,
                        month=month,
                        input_path=month_dir,
                        xml_files=xml_files,
                        output_path=(
                            config.ASSETS_DIR
                            / issuer_dir.name
                            / year_dir.name
                            / month
                        ),
                    )
                )

    logger.info(
        "Discovered %d raw partition(s) (before CLI filters).", len(partitions)
    )
    for p in partitions:
        logger.info(
            "  raw partition: issuer=%s year=%s month=%s files=%d",
            p.issuer_id, p.year, p.month, p.file_count,
        )
    return partitions


def filter_partitions(
    partitions: list[SourcePartition],
    issuer_ids: list[str] | None = None,
) -> list[SourcePartition]:
    """
    Keep only partitions whose issuer is in ``issuer_ids``.

    When ``issuer_ids`` is empty or None, returns an empty list (nothing runs).
    """
    if not issuer_ids:
        logger.warning(
            "PROCESS_ISSUERS is empty — no issuers selected. "
            "Edit PROCESS_ISSUERS in src/config.py to add issuer IDs."
        )
        return []

    allowed = {iid.strip() for iid in issuer_ids if iid.strip()}
    filtered = [p for p in partitions if p.issuer_id in allowed]
    logger.info(
        "PROCESS_ISSUERS filter %s → %d partition(s)",
        sorted(allowed),
        len(filtered),
    )

    found_issuers = {p.issuer_id for p in filtered}
    for iid in sorted(allowed):
        if iid not in found_issuers:
            logger.warning(
                "Issuer %s is in PROCESS_ISSUERS but has no valid "
                "source_data/%s/{year}/{month}/*.xml partitions",
                iid,
                iid,
            )

    skipped = {p.issuer_id for p in partitions} - allowed
    if skipped:
        logger.info(
            "Skipped issuer(s) not in PROCESS_ISSUERS: %s",
            sorted(skipped),
        )

    return filtered


def discover_partitions(source_root: Path | None = None) -> list[SourcePartition]:
    """
    Discover partitions in two steps: find all raw, then keep PROCESS_ISSUERS only.

    Calls ``log_source_tree_diagnostic()`` first so the console always shows
    the full folder tree before issuer filtering occurs.
    """
    root = source_root or config.SOURCE_DATA_DIR
    log_source_tree_diagnostic(root)
    all_partitions = discover_all_partitions(root)
    filtered = filter_partitions(all_partitions, config.PROCESS_ISSUERS)
    logger.info("Final partition count to process: %d", len(filtered))
    return filtered


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


def _is_valid_year(name: str) -> bool:
    """Return True when folder name is a four-digit year."""
    return len(name) == 4 and name.isdigit()


def _normalize_month(name: str) -> str | None:
    """
    Normalize month folder name to two digits (01-12).

    Accepts ``2``, ``02``, ``12`` etc. Returns None for invalid values.
    """
    if not name.isdigit():
        return None
    value = int(name)
    if value < 1 or value > 12:
        return None
    return str(value).zfill(2)


def _is_valid_month(name: str) -> bool:
    """Return True when folder name is a valid month (01-12)."""
    return _normalize_month(name) is not None
