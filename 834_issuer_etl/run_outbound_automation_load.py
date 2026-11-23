#!/usr/bin/env python3
"""
Outbound automation runner — TO 834 files -> Parser834Outbound -> dbo.outbound_automation*.

Independent of run_inbound_automation_load.py (inbound FROM files are never touched).

Examples:
    python run_outbound_automation_load.py --sftp-audit --issuer <HIOS> --year <YYYY> --month <M>
    python run_outbound_automation_load.py --download   --issuer <HIOS> --year <YYYY> --month <M>
    python run_outbound_automation_load.py --dry-run    --issuer <HIOS> --year <YYYY> --month <M>
    python run_outbound_automation_load.py --load       --issuer <HIOS> --year <YYYY> --month <M>
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from outbound_automation.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
