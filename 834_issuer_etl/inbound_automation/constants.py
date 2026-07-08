"""Constants for inbound automation runner."""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

PARSER_VERSION = "parser_834_staging_aligned"
RUNNER_VERSION = "1.0.0-phase1-dry-run"

OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "inbound_automation"

# Phase 1 supports dry-run only; load/create-table deferred to later phases.
SUPPORTED_MODES = frozenset({"dry_run"})
