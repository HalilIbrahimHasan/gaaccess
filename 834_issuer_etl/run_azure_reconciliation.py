#!/usr/bin/env python3
"""Azure vs XML reconciliation — delegates to intelligence pipeline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.intelligence_pipeline import run_intelligence_pipeline  # noqa: E402
from config.config import settings  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Azure vs XML reconciliation")
    parser.add_argument("--issuer", help="Filter by issuer")
    parser.add_argument("--year", help="Filter by year")
    parser.add_argument("--month", help="Filter by month")
    parser.add_argument("--skip-azure", action="store_true")
    parser.add_argument("--parse-source", action="store_true")
    opts = parser.parse_args()

    settings.apply_cli_filters(opts.issuer, opts.year, opts.month)
    settings.ensure_dirs()

    run_intelligence_pipeline(
        issuer_filter=settings.issuer_filter,
        year_filter=settings.year_filter,
        month_filter=settings.month_filter,
        prefer_staging=not opts.parse_source,
        skip_azure=opts.skip_azure or not settings.azure_enabled,
    )


if __name__ == "__main__":
    main()
