#!/usr/bin/env python3
"""Run load validation only."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from config.config import settings
from database.db import Database
from utils.logger import get_logger
from validation.load_validation import run_load_validation

logger = get_logger(__name__)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--issuer", default=None)
    args = p.parse_args()
    settings.ensure_dirs()
    db = Database()
    db.init_schema()
    run_load_validation(db, args.issuer)
    db.close()
    logger.info("Validation complete → %s/validation/", settings.reports_path)


if __name__ == "__main__":
    main()
