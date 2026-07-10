#!/usr/bin/env python3
"""
Verify 2026 source_data discovery completeness.

Compares:
  - physical XML files on disk (same partition rules as file_discovery.py)
  - discover_for_run() output (pipeline discovery)
  - Azure dbo.inbound_automation_file_log status (optional, when Azure configured)

Usage:
    python run_verify_2026_discovery.py
    python run_verify_2026_discovery.py --source-root /path/to/source_data
    python run_verify_2026_discovery.py --no-azure
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.config import settings
from inbound_automation.discovery import discover_for_run
from ingestion.file_discovery import ARCHIVE_EXTS, XML_EXTS, discover_source_files
from ingestion.zip_handler import expand_archives

YEAR = "2026"


def _issuer_ok(name: str) -> bool:
    return name.isdigit() and len(name) == 5 and not name.startswith(".")


def _normalize_year(name: str) -> str | None:
    return name if name.isdigit() and len(name) == 4 else None


def _normalize_month(name: str) -> str | None:
    if not name.isdigit() or len(name) > 2:
        return None
    m = int(name)
    return str(m).zfill(2) if 1 <= m <= 12 else None


def scan_physical_xml_paths(source_root: Path) -> set[str]:
    """
    Independent filesystem scan mirroring ingestion/file_discovery.py rules.

    Walks source_data/{issuer}/{year}/{month}/ with rglob; includes XML inside zips.
    """
    paths: set[str] = set()
    if not source_root.exists():
        return paths

    for issuer_dir in sorted(source_root.iterdir(), key=lambda p: p.name):
        if not issuer_dir.is_dir() or not _issuer_ok(issuer_dir.name):
            continue
        for year_dir in sorted(issuer_dir.iterdir(), key=lambda p: p.name):
            if not year_dir.is_dir():
                continue
            year = _normalize_year(year_dir.name)
            if year != YEAR:
                continue
            for month_dir in sorted(year_dir.iterdir(), key=lambda p: p.name):
                if not month_dir.is_dir():
                    continue
                month = _normalize_month(month_dir.name)
                if not month:
                    continue
                for path in sorted(month_dir.rglob("*")):
                    if not path.is_file() or path.name.startswith("."):
                        continue
                    suffix = path.suffix.lower()
                    if suffix in ARCHIVE_EXTS:
                        for extracted in expand_archives(
                            path, None, issuer_dir.name, year, month,
                        ):
                            if extracted.suffix.lower() in XML_EXTS:
                                paths.add(str(extracted.resolve()))
                        continue
                    if suffix in XML_EXTS:
                        paths.add(str(path.resolve()))
    return paths


def discovered_paths(source_root: Path) -> set[str]:
    """Pipeline discovery for folder_year=2026 (all issuers, all months)."""
    sources = discover_for_run(
        source_root,
        year_filter=YEAR,
        all_years=False,
        issuer_filter=None,
        month_filter=None,
    )
    return {str(s.file_path.resolve()) for s in sources}


def query_azure_file_log() -> dict[str, int]:
    from sqlalchemy import text

    from inbound_automation.azure_common import connect_automation_engine

    engine = connect_automation_engine()
    sql = text("""
        SELECT parse_status, COUNT(*) AS cnt
        FROM dbo.inbound_automation_file_log
        WHERE folder_year = :year
        GROUP BY parse_status
        ORDER BY parse_status
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"year": int(YEAR)}).fetchall()
    return {str(row[0]): int(row[1]) for row in rows}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify 2026 XML discovery completeness")
    parser.add_argument(
        "--source-root",
        type=Path,
        default=None,
        help="Override source_data root (default: settings.source_data_path)",
    )
    parser.add_argument(
        "--no-azure",
        action="store_true",
        help="Skip Azure file_log query",
    )
    args = parser.parse_args(argv)

    settings.refresh_from_env()
    source_root = (args.source_root or settings.source_data_path).resolve()

    print(f"Source root: {source_root}")
    print(f"Folder year filter: {YEAR}")
    print()

    physical = scan_physical_xml_paths(source_root)
    discovered = discovered_paths(source_root)

    only_physical = sorted(physical - discovered)
    only_discovered = sorted(discovered - physical)

    print("=" * 60)
    print("COUNTS")
    print("=" * 60)
    print(f"  Physical XML (independent scan):  {len(physical):,}")
    print(f"  Discovered (discover_for_run):    {len(discovered):,}")
    print(f"  Delta (physical - discovered):    {len(physical) - len(discovered):+,}")
    print()

    if only_physical:
        print(f"IN PHYSICAL ONLY ({len(only_physical)}) — not discovered:")
        for p in only_physical[:20]:
            print(f"  {p}")
        if len(only_physical) > 20:
            print(f"  ... and {len(only_physical) - 20} more")
        print()

    if only_discovered:
        print(f"IN DISCOVERED ONLY ({len(only_discovered)}) — not in physical scan:")
        for p in only_discovered[:20]:
            print(f"  {p}")
        if len(only_discovered) > 20:
            print(f"  ... and {len(only_discovered) - 20} more")
        print()

    match = physical == discovered
    print("=" * 60)
    if match:
        print("CONFIRMED: physical XML count = discovered count")
    else:
        print("MISMATCH: physical XML count != discovered count")
        print(f"  Only in physical:   {len(only_physical):,}")
        print(f"  Only in discovered: {len(only_discovered):,}")
    print("=" * 60)
    print()

    if not args.no_azure:
        print("Azure dbo.inbound_automation_file_log (folder_year=2026):")
        try:
            status_counts = query_azure_file_log()
            loaded = status_counts.get("loaded", 0)
            failed = status_counts.get("failed", 0)
            skipped_dup = status_counts.get("skipped_duplicate", 0)
            other = {
                k: v for k, v in status_counts.items()
                if k not in ("loaded", "failed", "skipped_duplicate")
            }
            total_logged = sum(status_counts.values())

            print(f"  loaded:              {loaded:,}")
            print(f"  failed:              {failed:,}")
            print(f"  skipped_duplicate:   {skipped_dup:,}")
            for status, cnt in sorted(other.items()):
                print(f"  {status}:{' ' * (18 - len(status))}{cnt:,}")
            print(f"  total file_log rows: {total_logged:,}")
            print()
            print(
                "Note: skipped_duplicate files are not written to file_log during load; "
                "they reuse an existing loaded file_hash. Use run reports for skip counts."
            )
            print(
                f"Reconciliation check: loaded + failed + skipped_dup = "
                f"{loaded + failed + skipped_dup:,}"
            )
        except SystemExit:
            print("  (Azure not configured — skipped)")
        except Exception as exc:
            print(f"  Azure query failed: {exc}")

    return 0 if match else 1


if __name__ == "__main__":
    raise SystemExit(main())
