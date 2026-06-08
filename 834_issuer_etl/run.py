#!/usr/bin/env python3
"""
Run the 834 Issuer ETL from any working directory.

    cd 834_issuer_etl
    python run.py

Or from the parent repo:

    python 834_issuer_etl/run.py
"""
from __future__ import annotations

import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from main import main  # noqa: E402

if __name__ == "__main__":
    main()
