"""CLI for the outbound automation runner (TO files -> dbo.outbound_automation*)."""

from __future__ import annotations

import argparse
import sys

from ingestion.sftp_filters import parse_csv_filter
from outbound_automation.config import (
    OutboundConfigError,
    resolve_outbound_remote_path,
    resolve_outbound_source_root,
    resolve_sftp_credentials,
    resolve_sftp_reliability_settings,
    resolve_source_json_policy,
)
from outbound_automation.constants import ENV_ENABLED, ENV_SOURCE_JSON_PII_POLICY, OUTPUT_ROOT
from outbound_automation.run_context import OutboundRunContext, new_load_run_id
from utils.logger import get_logger

logger = get_logger(__name__)

_MODES = ("sftp_audit", "download", "dry_run", "load")


def _parse_issuer_list(values: list[str] | None) -> list[str] | None:
    if not values:
        return None
    issuers = [p.strip() for v in values for p in v.split(",") if p.strip()]
    return issuers or None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Outbound automation — TO 834 files -> Parser834Outbound -> "
            "dbo.outbound_automation + dbo.outbound_automation_monthly_premium."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Steps (each is a separate, explicit invocation):
  python run_outbound_automation_load.py --sftp-audit --issuer <HIOS> --year <YYYY> --month <M>
  python run_outbound_automation_load.py --download   --issuer <HIOS> --year <YYYY> --month <M>
  python run_outbound_automation_load.py --dry-run    --issuer <HIOS> --year <YYYY> --month <M>
  python run_outbound_automation_load.py --load       --issuer <HIOS> --year <YYYY> --month <M>

--load requires {ENV_ENABLED}=true and an approved {ENV_SOURCE_JSON_PII_POLICY} (full | redact).
        """.strip(),
    )
    parser.add_argument("--sftp-audit", action="store_true",
                        help="List remote TO files under SFTP_OUTBOUND_REMOTE_PATH (no download).")
    parser.add_argument("--download", action="store_true",
                        help="Download remote TO files into the outbound source root.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse local outbound TO files and write Excel reports (no Azure writes).")
    parser.add_argument("--load", action="store_true",
                        help=f"Insert into dbo.outbound_automation* (requires {ENV_ENABLED}=true).")
    parser.add_argument("--year", type=str, help="Year filter (required unless --all-years).")
    parser.add_argument("--all-years", action="store_true", help="All year folders.")
    parser.add_argument("--issuer", action="append", metavar="ISSUER",
                        help="Issuer HIOS id filter (repeatable or comma-separated).")
    parser.add_argument("--month", type=str, help="Month filter (1-12). Default: all months.")
    parser.add_argument("--force-download", action="store_true",
                        help="--download only: re-download even if the local .xml exists.")
    parser.add_argument("--keep-compressed", action="store_true",
                        help="--download only: keep .xml.gz/.xml.xz next to the decompressed .xml.")
    return parser


def _selected_modes(args: argparse.Namespace) -> list[str]:
    return [m for m in _MODES if getattr(args, m)]


def _validate_args(args: argparse.Namespace) -> str:
    modes = _selected_modes(args)
    if not modes:
        raise SystemExit(
            "Error: explicit mode required (--sftp-audit | --download | --dry-run | --load)."
        )
    if len(modes) > 1:
        raise SystemExit(f"Error: mutually exclusive modes: {', '.join(modes)}")
    if not args.all_years and not args.year:
        raise SystemExit("Error: --year is required unless --all-years is set.")
    if args.all_years and args.year:
        raise SystemExit("Error: --year and --all-years are mutually exclusive.")
    if args.month:
        try:
            if not 1 <= int(args.month) <= 12:
                raise ValueError
        except ValueError:
            raise SystemExit("Error: --month must be an integer between 1 and 12.")
    if (args.force_download or args.keep_compressed) and modes[0] != "download":
        raise SystemExit("Error: --force-download / --keep-compressed apply to --download only.")
    return modes[0]


def _run_sftp(args: argparse.Namespace, *, download: bool) -> int:
    from outbound_automation.sftp_download import run_outbound_sftp

    remote_root = resolve_outbound_remote_path()
    local_root = resolve_outbound_source_root()
    creds = resolve_sftp_credentials()
    reliability = resolve_sftp_reliability_settings()
    issuers = _parse_issuer_list(args.issuer)
    output_dir = OUTPUT_ROOT / f"{new_load_run_id()}_sftp_{'download' if download else 'audit'}"

    print(f"\nOUTBOUND AUTOMATION — SFTP {'DOWNLOAD' if download else 'AUDIT (no download)'}")
    print(f"  remote root     : {remote_root}")
    print(f"  local root      : {local_root}{'' if download else ' (not written)'}")
    print("  file direction  : TO files only")
    print(f"  issuer filter   : {', '.join(issuers) if issuers else 'ALL'}")
    print(f"  year filter     : {args.year or 'ALL'}")
    print(f"  month filter    : {args.month or 'ALL'}")
    print(f"  reports         : {output_dir}")
    print("  SFTP reliability:")
    for label, value in reliability.describe():
        print(f"    {label:<20}: {value}")

    run = run_outbound_sftp(
        creds=creds,
        remote_root=remote_root,
        local_root=local_root,
        output_dir=output_dir,
        issuer_allow=set(issuers) if issuers else None,
        year_allow=None if args.all_years else {args.year},
        month_allow=parse_csv_filter(args.month, normalizer=lambda m: str(int(m)).zfill(2)),
        download=download,
        force_download=args.force_download,
        keep_compressed=args.keep_compressed,
        settings=reliability,
    )
    results = run.results

    print("\nOutbound SFTP complete." if not run.failed else "\nOutbound SFTP finished WITH FAILURES.")
    print(f"  Partitions                  : {len(results)}")
    print(f"  Partition listings failed   : {sum(1 for r in results if r.listing_failed)}")
    print(f"  Remote files scanned        : {sum(r.files_scanned for r in results)}")
    print(f"  TO files selected           : {sum(r.valid_total for r in results)}")
    if download:
        print(f"  Downloaded                  : {sum(r.downloaded for r in results)}")
        print(f"  Skipped already downloaded  : {sum(r.existing for r in results)}")
        print(f"  Re-downloaded               : {sum(r.redownloaded for r in results)}")
        print(f"  Failed                      : {sum(r.failed for r in results)}")
    print(f"  Stall retries               : {run.stats.stall_retries}")
    print(f"  Reconnects                  : {run.stats.reconnects}")
    for error in run.run_errors:
        print(f"  ERROR: {error}")
    for name, path in run.paths.items():
        print(f"  {name}: {path}")
    if run.failed:
        if download:
            print("\nCompleted files were kept. Re-run the same command to continue; "
                  "verified files are skipped (SKIP_ALREADY_DOWNLOADED).")
        return 1
    return 0


def _print_footer(result) -> int:
    from outbound_automation.reports import write_run_reports

    print("\nRun complete.")
    print(f"  source_xml_json policy  : {result.pii_policy}")
    print(f"  Files discovered        : {result.files_discovered}")
    print(f"  Files rejected (name)   : {len(result.rejected_files)}")
    print(f"  Files parsed            : {result.files_parsed}")
    print(f"  Files loaded            : {result.files_loaded}")
    print(f"  Files skipped (dup hash): {result.files_skipped_duplicate}")
    print(f"  Files failed            : {result.files_failed}")
    print(f"  Rows parsed             : {result.rows_parsed}")
    print(f"  Premium rows parsed     : {result.premium_rows_parsed}")
    print(f"  Rows inserted           : {result.rows_inserted}")
    print(f"  Premium rows inserted   : {result.premium_rows_inserted}")
    print(f"  Warnings (total)        : {result.total_warning_count}")
    print(f"  Unknown XML paths       : {result.unknown_path_count}")
    print(f"  Reports                 : {result.context.output_dir}")
    for name, path in write_run_reports(result).items():
        print(f"    {name}: {path}")
    if result.files_discovered == 0:
        print("\nNo outbound TO files matched filters.")
        return 1
    if result.files_failed:
        print("\nOne or more files failed (see file_summary.xlsx); they remain retryable.")
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    mode = _validate_args(args)

    try:
        if mode == "sftp_audit":
            return _run_sftp(args, download=False)
        if mode == "download":
            return _run_sftp(args, download=True)

        from outbound_automation.pipeline import run_dry_run, run_load

        policy = resolve_source_json_policy(required=(mode == "load"))
        context = OutboundRunContext.create(
            run_mode=mode,
            year_filter=args.year,
            all_years=args.all_years,
            issuer_filter=_parse_issuer_list(args.issuer),
            month_filter=args.month,
            source_root=resolve_outbound_source_root(),
        )
    except OutboundConfigError as exc:
        print(f"Outbound configuration error: {exc}", file=sys.stderr)
        return 2

    issuers = context.issuer_filter
    print(f"\nOUTBOUND AUTOMATION — {mode.upper().replace('_', ' ')}")
    print(f"  load_run_id     : {context.load_run_id}")
    print(f"  source root     : {context.source_root}")
    print(f"  year filter     : {context.year_filter or 'ALL'}")
    print(f"  issuer filter   : {', '.join(issuers) if issuers else 'ALL'}")
    print(f"  month filter    : {context.month_filter or 'ALL'}")
    print(f"  output dir      : {context.output_dir}")
    print(f"  JSON PII policy : {policy.name}")
    if mode == "load":
        print("  Azure writes    : dbo.outbound_automation + monthly_premium + run_log + file_log")
        return _print_footer(run_load(context, policy=policy))
    print("  Azure writes    : NONE")
    return _print_footer(run_dry_run(context, policy=policy))


if __name__ == "__main__":
    sys.exit(main())
