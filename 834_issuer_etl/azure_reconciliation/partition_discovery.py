"""
Discover issuer/year/month partitions from source_data only.

Never reads from assets/. Fully dynamic — no hardcoded issuers or months.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from config.config import settings
from ingestion.file_discovery import _issuer_ok, _normalize_month, _normalize_year
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class Partition:
    issuer: str
    year: str
    month: str

    @property
    def sort_key(self) -> tuple[int, int, int]:
        return (int(self.year), int(self.month), int(self.issuer))

    @property
    def path(self) -> Path:
        return settings.source_data_path / self.issuer / self.year / self.month

    def label(self) -> str:
        return f"{self.issuer}/{self.year}/{self.month}"


def _parse_filter_set(raw: str | None) -> set[str] | None:
    if not raw or not str(raw).strip():
        return None
    return {p.strip() for p in str(raw).split(",") if p.strip()}


def discover_partitions(
    source_root: Path | None = None,
    *,
    issuer_filter: str | None = None,
    year_filter: str | None = None,
    month_filter: str | None = None,
) -> list[Partition]:
    """
    Walk source_data/{issuer}/{year}/{month}/ and return sorted partitions.

    Applies .env / CLI filters when provided.
    """
    root = source_root or settings.source_data_path
    issuer_allow = _parse_filter_set(issuer_filter or settings.issuer_filter)
    year_allow = _parse_filter_set(year_filter or settings.year_filter)
    month_allow = _parse_filter_set(month_filter or settings.month_filter)
    if month_allow:
        month_allow = {m.zfill(2) if m.isdigit() else m for m in month_allow}

    partitions: list[Partition] = []
    if not root.exists():
        logger.warning("source_data root missing: %s", root)
        return partitions

    for issuer_dir in sorted(root.iterdir(), key=lambda p: p.name):
        if not issuer_dir.is_dir() or not _issuer_ok(issuer_dir.name):
            continue
        if issuer_allow and issuer_dir.name not in issuer_allow:
            continue

        for year_dir in sorted(issuer_dir.iterdir(), key=lambda p: p.name):
            if not year_dir.is_dir():
                continue
            year = _normalize_year(year_dir.name)
            if not year:
                continue
            if year_allow and year not in year_allow:
                continue

            for month_dir in sorted(year_dir.iterdir(), key=lambda p: p.name):
                if not month_dir.is_dir():
                    continue
                month = _normalize_month(month_dir.name)
                if not month:
                    continue
                if month_allow and month not in month_allow:
                    continue
                partitions.append(Partition(issuer_dir.name, year, month))

    partitions.sort(key=lambda p: p.sort_key)
    issuers = sorted({p.issuer for p in partitions})
    years = sorted({p.year for p in partitions})
    months = sorted({p.month for p in partitions})
    logger.info(
        "source_data discovered: %d partition(s), issuers=%s years=%s months=%s",
        len(partitions),
        issuers or "[]",
        years or "[]",
        months or "[]",
    )
    return partitions
