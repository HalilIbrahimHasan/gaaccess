"""Constants for outbound automation runner (TO files -> dbo.outbound_automation*)."""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_DIR = Path(__file__).resolve().parent

PARSER_VERSION = "parser_834_outbound_v1"
RUNNER_VERSION = "2.0.0-outbound"

OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "outbound_automation"

RUN_ID_PREFIX = "outbound"

SCHEMA = "dbo"
OUTBOUND_TABLE = "outbound_automation"
OUTBOUND_MONTHLY_PREMIUM_TABLE = "outbound_automation_monthly_premium"
OUTBOUND_RUN_LOG_TABLE = "outbound_automation_run_log"
OUTBOUND_FILE_LOG_TABLE = "outbound_automation_file_log"
OUTBOUND_TABLES = (
    OUTBOUND_TABLE,
    OUTBOUND_MONTHLY_PREMIUM_TABLE,
    OUTBOUND_RUN_LOG_TABLE,
    OUTBOUND_FILE_LOG_TABLE,
)

# Outbound-only environment variables (never the inbound SFTP_REMOTE_PATH / SOURCE_DATA_PATH).
ENV_REMOTE_PATH = "SFTP_OUTBOUND_REMOTE_PATH"
ENV_SOURCE_DATA_PATH = "OUTBOUND_SOURCE_DATA_PATH"
ENV_ENABLED = "OUTBOUND_AUTOMATION_ENABLED"
ENV_BATCH_SIZE = "OUTBOUND_AUTOMATION_BATCH_SIZE"
ENV_FAST_EXECUTEMANY = "OUTBOUND_AUTOMATION_FAST_EXECUTEMANY"
ENV_EXCHANGE_CODES = "OUTBOUND_EXCHANGE_CODES"
ENV_SOURCE_JSON_PII_POLICY = "OUTBOUND_SOURCE_JSON_PII_POLICY"
ENV_INSURANCE_TYPE_MAP_PATH = "OUTBOUND_INSURANCE_TYPE_MAP_PATH"

# Exchange code(s) accepted in to_<issuer>_<exchange>_834_INDV_<timestamp>.xml.
# Default mirrors the documented inbound FROM rule (from_{issuer}_GA_834_INDV_*);
# override with OUTBOUND_EXCHANGE_CODES=GA,XX.
DEFAULT_EXCHANGE_CODES = ("GA",)

# source_xml_json PII policies. There is deliberately no default: --load refuses to
# run until OUTBOUND_SOURCE_JSON_PII_POLICY is set to an approved value.
PII_POLICY_FULL = "full"
PII_POLICY_REDACT = "redact"
PII_POLICIES = (PII_POLICY_FULL, PII_POLICY_REDACT)
PII_POLICY_UNSET_DRY_RUN = "unset_dry_run_in_memory_only"

KNOWN_PATHS_FILE = PACKAGE_DIR / "known_paths_v1.json"
INSURANCE_TYPE_MAP_FILE = PACKAGE_DIR / "insurance_type_map_v1.json"
SENSITIVE_TAGS_FILE = PACKAGE_DIR / "sensitive_tags_v1.json"

# Subfolder of the existing SOURCE_DATA_PATH. Inbound discovery only walks 5-digit
# issuer folders, so this folder is never visited by the inbound pipeline.
DEFAULT_SOURCE_SUBDIR = "outbound"

SUPPORTED_MODES = frozenset({"sftp_audit", "download", "dry_run", "load"})
