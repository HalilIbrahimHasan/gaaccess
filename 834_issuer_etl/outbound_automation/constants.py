"""Constants for outbound automation runner (TO files -> dbo.outbound_automation*)."""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

PARSER_VERSION = "parser_834_staging_aligned"
RUNNER_VERSION = "1.0.0-outbound"

OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "outbound_automation"

RUN_ID_PREFIX = "outbound"

SCHEMA = "dbo"
OUTBOUND_TABLE = "outbound_automation"
OUTBOUND_RUN_LOG_TABLE = "outbound_automation_run_log"
OUTBOUND_FILE_LOG_TABLE = "outbound_automation_file_log"
OUTBOUND_TABLES = (
    OUTBOUND_TABLE,
    OUTBOUND_RUN_LOG_TABLE,
    OUTBOUND_FILE_LOG_TABLE,
)

# Outbound-only environment variables (never the inbound SFTP_REMOTE_PATH / SOURCE_DATA_PATH).
ENV_REMOTE_PATH = "SFTP_OUTBOUND_REMOTE_PATH"
ENV_SOURCE_DATA_PATH = "OUTBOUND_SOURCE_DATA_PATH"
ENV_ENABLED = "OUTBOUND_AUTOMATION_ENABLED"
ENV_BATCH_SIZE = "OUTBOUND_AUTOMATION_BATCH_SIZE"
ENV_FAST_EXECUTEMANY = "OUTBOUND_AUTOMATION_FAST_EXECUTEMANY"

# Subfolder of the existing SOURCE_DATA_PATH. Inbound discovery only walks 5-digit
# issuer folders, so this folder is never visited by the inbound pipeline.
DEFAULT_SOURCE_SUBDIR = "outbound"

SUPPORTED_MODES = frozenset({"sftp_audit", "download", "dry_run", "load"})
