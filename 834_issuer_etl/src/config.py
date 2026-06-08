"""
Central configuration for the 834 Issuer ETL framework.

This module defines paths, column schemas, PII policy, and validation rules
so that all pipeline stages share a single source of truth.
"""

from pathlib import Path

# ---------------------------------------------------------------------------
# Project paths (resolved relative to the 834_issuer_etl package root)
# ---------------------------------------------------------------------------
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
SOURCE_DATA_DIR: Path = PROJECT_ROOT / "source_data"
ASSETS_DIR: Path = PROJECT_ROOT / "assets"

# ---------------------------------------------------------------------------
# PII handling — default is safe; set EXPORT_PII=True only for debugging
# ---------------------------------------------------------------------------
EXPORT_PII: bool = False

PII_COLUMNS: list[str] = [
    "member_ssn",
    "member_primary_phone_no",
    "member_preferred_email",
    "member_first_name",
    "member_last_name",
    "member_full_address",
]

# ---------------------------------------------------------------------------
# Required output columns (post-cleaning / standardization)
# ---------------------------------------------------------------------------
REQUIRED_COLUMNS: list[str] = [
    "source_file",
    "issuer_id",
    "isa09",
    "isa10",
    "isa13",
    "gs04",
    "gs05",
    "gs06",
    "st02",
    "action_code",
    "insurer_tax_id_number",
    "qtyn",
    "qtyy",
    "qtyt",
    "subscriber_flag",
    "relationship_code",
    "event_type_code",
    "event_reason_code",
    "exchg_subscriber_identifier",
    "exchg_assigned_policy_id",
    "exchg_indiv_identifier",
    "issuer_subscriber_identifier",
    "issuer_indiv_identifier",
    "member_maint_effective_date",
    "member_entity_identifier_code",
    "member_gender_code",
    "member_marital_status_code",
    "member_citizenship_status_code",
    "member_tobacco_usage_code",
    "city",
    "state",
    "zip",
    "member_birth_date",
    "maintenance_type_code",
    "insurance_type_code",
    "benefit_effective_begin_date",
    "last_premium_paid_date",
    "household_or_employee_case_id",
    "class_of_contract_code",
    "health_coverage_policy_no",
    "aptc_amt",
    "health_coverage_premium_amt",
    "rating_area",
    "total_indiv_responsibility_amt",
    "total_premium_amt",
    "source_exchg_id",
    "additional_maint_reason_code",
    "load_timestamp",
    "file_date",
]

# ID fields that must not be null for a valid enrollee record
REQUIRED_ID_FIELDS: list[str] = [
    "issuer_id",
    "exchg_indiv_identifier",
    "exchg_assigned_policy_id",
]

# Date columns stored as YYYYMMDD in source XML, converted during cleaning
DATE_COLUMNS: list[str] = [
    "member_maint_effective_date",
    "member_birth_date",
    "benefit_effective_begin_date",
    "last_premium_paid_date",
    "file_date",
]

# Numeric / amount columns converted to float during cleaning
NUMERIC_COLUMNS: list[str] = [
    "qtyn",
    "qtyy",
    "qtyt",
    "aptc_amt",
    "health_coverage_premium_amt",
    "total_indiv_responsibility_amt",
    "total_premium_amt",
]

# Valid subscriber flag values
VALID_SUBSCRIBER_FLAGS: set[str] = {"Y", "N"}

# Default issuer used in examples / CLI help text only — not hardcoded in logic
DEFAULT_ISSUER_EXAMPLE: str = "64357"

# SQLite table names
TABLE_ENROLLEES: str = "issuer_enrollees"
TABLE_KPIS: str = "issuer_kpis"
TABLE_VALIDATION: str = "validation_results"

# Logging
LOG_FORMAT: str = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOG_LEVEL: str = "INFO"
