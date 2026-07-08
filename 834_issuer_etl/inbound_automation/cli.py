"""CLI argument parsing for inbound automation runner."""

from __future__ import annotations

import argparse
import sys

from inbound_automation.constants import SUPPORTED_MODES
from inbound_automation.pipeline import run_dry_run
from inbound_automation.reports import write_dry_run_reports
from inbound_automation.run_context import LoadRunContext
from utils.logger import get_logger

logger = get_logger(__name__)


def _parse_issuer_list(values: list[str] | None) -> list[str] | None:
    if not values:
        return None
    issuers: list[str] = []
    for value in values:
        for part in value.split(","):
            part = part.strip()
            if part:
                issuers.append(part)
    return issuers or None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inbound automation — Parser834 raw ingestion to Azure SQL.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples (Phase 1 — dry-run only):
  python run_inbound_automation_load.py --year 2025 --dry-run
  python run_inbound_automation_load.py --issuer 15105 --year 2025 --month 10 --dry-run
  python run_inbound_automation_load.py --all-years --dry-run
        """.strip(),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover, parse, enrich, and write Excel reports only (no Azure writes).",
    )
    parser.add_argument(
        "--create-table",
        action="store_true",
        help="[Phase 2+] Create inbound_automation tables in Azure SQL.",
    )
    parser.add_argument(
        "--load",
        action="store_true",
        help="[Phase 2+] Insert parsed rows into Azure SQL.",
    )
    parser.add_argument(
        "--year",
        type=str,
        help="Coverage/partition year filter (required unless --all-years).",
    )
    parser.add_argument(
        "--all-years",
        action="store_true",
        help="Discover all year folders under source_data.",
    )
    parser.add_argument(
        "--issuer",
        action="append",
        metavar="ISSUER",
        help="Issuer HIOS id filter (repeatable or comma-separated). Default: all issuers.",
    )
    parser.add_argument(
        "--month",
        type=str,
        help="Month filter (1-12). Default: all months.",
    )
    parser.add_argument(
        "--source",
        choices=["local", "sftp"],
        default="local",
        help="Source mode (v1: local only).",
    )
    return parser


def _validate_args(args: argparse.Namespace) -> None:
    modes = [name for name, enabled in (
        ("dry_run", args.dry_run),
        ("create_table", args.create_table),
        ("load", args.load),
    ) if enabled]

    if not modes:
        raise SystemExit(
            "Error: explicit mode required. Phase 1 supports --dry-run only.\n"
            "Example: python run_inbound_automation_load.py --year 2025 --dry-run"
        )
    if len(modes) > 1:
        raise SystemExit(f"Error: mutually exclusive modes: {', '.join(modes)}")

    if args.create_table or args.load:
        raise SystemExit(
            "Error: --create-table and --load are not implemented yet (Phase 2+).\n"
            "Phase 1 supports --dry-run only."
        )

    if not args.all_years and not args.year:
        raise SystemExit("Error: --year is required unless --all-years is set.")

    if args.all_years and args.year:
        raise SystemExit("Error: --year and --all-years are mutually exclusive.")

    if args.month:
        try:
            month = int(args.month)
            if not 1 <= month <= 12:
                raise ValueError
        except ValueError:
            raise SystemExit("Error: --month must be an integer between 1 and 12.")

    if args.source == "sftp":
        raise SystemExit("Error: --source sftp is not implemented in Phase 1.")

    if "dry_run" not in SUPPORTED_MODES and args.dry_run:
        raise SystemExit("Error: dry-run mode is not enabled in this build.")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _validate_args(args)

    issuer_filter = _parse_issuer_list(args.issuer)
    context = LoadRunContext.create(
        run_mode="dry_run",
        year_filter=args.year,
        all_years=args.all_years,
        issuer_filter=issuer_filter,
        month_filter=args.month,
        source_mode=args.source,
    )

    print("\nINBOUND AUTOMATION — DRY RUN (Phase 1)")
    print(f"  load_run_id     : {context.load_run_id}")
    print(f"  year filter     : {context.year_filter or 'ALL'}")
    print(f"  issuer filter   : {', '.join(issuer_filter) if issuer_filter else 'ALL'}")
    print(f"  month filter    : {context.month_filter or 'ALL'}")
    print(f"  output dir      : {context.output_dir}")
    print("  Azure writes    : NONE")
    print("  Parser834       : unmodified raw parse")

    result = run_dry_run(context)
    report_paths = write_dry_run_reports(result)

    print("\nDry-run complete.")
    print(f"  Files discovered : {result.files_discovered}")
    print(f"  Files parsed     : {result.files_parsed}")
    print(f"  Files failed     : {result.files_failed}")
    print(f"  Rows parsed      : {result.rows_parsed}")
    print(f"  Warnings (rows)  : {result.total_warning_count}")
    print(f"  Reports          : {context.output_dir}")
    for name, path in report_paths.items():
        print(f"    {name}: {path}")

    if result.files_discovered == 0:
        print("\nNo source files matched filters.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
