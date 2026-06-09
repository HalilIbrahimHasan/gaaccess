#!/usr/bin/env python3
"""Run KPI / reconciliation reports only."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from config.config import settings
from database.db import Database
from reconciliation.business_rules import apply_business_rules
from reconciliation.premium_validation import apply_premium_validation
from reconciliation.user_fee_calculation import apply_user_fees
from reporting.report_runner import run_kpi_reports
from utils.logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--issuer", default=None)
    args = p.parse_args()
    settings.ensure_dirs()
    db = Database()
    db.init_schema()
    apply_premium_validation(db)
    apply_user_fees(db)
    apply_business_rules(db)
    run_kpi_reports(db, args.issuer)
    db.close()
    logger.info("KPI reports complete → %s/kpi/", settings.reports_path)


if __name__ == "__main__":
    main()
