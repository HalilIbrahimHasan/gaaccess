"""
Outbound enrichment: parsed TO records -> V2 dbo.outbound_automation rows and
dbo.outbound_automation_monthly_premium child rows.

Rules:
  * coverage_year comes only from benefitEffectiveBeginDate (never CLI / folder /
    filename year, which remain lineage columns).
  * insurance_type comes from the configured map; unknown code -> NULL + warning.
  * outbound_transaction_type comes from outbound_automation.mappings.
  * warnings are codes only (never source values).
  * missing member or policy identity, or a value wider than its V2 column,
    raises OutboundFileFatalError (whole file fails, nothing is loaded).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from outbound_automation.mappings import classify_outbound_transaction, map_insurance_type
from outbound_automation.schema_observation import FileStructure, SchemaObservationCollector
from outbound_automation.schema_v2 import (
    FILE_LOG_JOB_EXECUTION_ID_MAX,
    MONTHLY_PREMIUM_MAX_LENGTHS,
    OUTBOUND_AUTOMATION_MAX_LENGTHS,
    WARNING_CODES_MAX,
)
from outbound_automation.source_json import (
    SourceJsonPolicy,
    serialize_enrollee,
    serialize_premium,
)
from parsers.parser_834_outbound import (
    OutboundEnrolleeRecord,
    OutboundEnrollmentRecord,
    OutboundParseResult,
)

COVERAGE_YEAR_SOURCE_BENEFIT = "benefit_effective_begin_date"
COVERAGE_YEAR_SOURCE_UNRESOLVED = "unresolved"

W_COVERAGE_YEAR_UNRESOLVED = "COVERAGE_YEAR_UNRESOLVED"
W_MISSING_ENROLLMENT_EVENT_ID = "MISSING_ENROLLMENT_EVENT_ID"
W_POLICY_ID_MISMATCH = "POLICY_ID_MISMATCH"
W_EVENT_MAINT_TYPE_MISMATCH = "EVENT_MAINT_TYPE_MISMATCH"
W_EVENT_MAINT_REASON_MISMATCH = "EVENT_MAINT_REASON_MISMATCH"
W_CLASS_PLAN_MISMATCH = "CLASS_PLAN_MISMATCH"
W_PLAN_ISSUER_MISMATCH = "PLAN_ISSUER_MISMATCH"
W_QTYT_MISMATCH = "QTYT_MISMATCH"
W_QTYY_MISMATCH = "QTYY_MISMATCH"
W_QTYN_MISMATCH = "QTYN_MISMATCH"
W_MULTIPLE_SUBSCRIBERS = "MULTIPLE_SUBSCRIBERS_IN_ENROLLMENT"
W_DUPLICATE_PREMIUM_MONTH = "DUPLICATE_PREMIUM_MONTH"
W_PREMIUM_PERIOD_MISMATCH = "PREMIUM_PERIOD_MISMATCH"
W_HEADER_ISSUER_MISSING = "HEADER_ISSUER_MISSING"
W_INVALID_TXN_CREATE_DATETIME = "INVALID_TXN_CREATE_DATETIME"
W_WARNING_CODES_TRUNCATED = "WARNING_CODES_TRUNCATED"

_DATE_COLUMNS = (
    "benefit_effective_date",
    "benefit_end_date",
    "member_maint_effective_date",
    "last_premium_paid_date",
)
_AMOUNT_COLUMNS = (
    "total_premium_amount",
    "individual_responsibility_amount",
    "aptc_amount",
    "health_coverage_premium_amount",
)
_PASSTHROUGH_COLUMNS = (
    "enrollment_event_id",
    "enrollee_event_type_code",
    "enrollee_event_reason_code",
    "maintenance_type_code",
    "additional_maint_reason_code",
    "request_submit_timestamp",
    "payment_txn_id",
    "previous_policy_id",
    "member_id",
    "issuer_subscriber_identifier",
    "issuer_indiv_identifier",
    "subscriber_flag",
    "relationship",
    "member_first_name",
    "member_last_name",
    "insurance_type_code",
    "class_of_contract_code",
    "health_coverage_policy_no",
    "household_or_employee_case_id",
    "rating_area",
    "source_exchg_id",
)
_PREMIUM_AMOUNT_FIELDS = ("gross_premium_amount", "aptc_amount", "net_premium_amount")

_DATE8_RE = re.compile(r"^\d{8}$")
_DATETIME14_RE = re.compile(r"^\d{14}$")
_INT_RE = re.compile(r"^[+-]?\d+$")
_DECIMAL_LIMIT = Decimal("1e14")  # DECIMAL(18,4)


class OutboundFileFatalError(ValueError):
    """The whole file must fail (no rows from it are loaded)."""


# ---------------------------------------------------------------------- typing
def parse_date(raw: str | None) -> tuple[date | None, bool]:
    """Return (value, ok). Accepts YYYYMMDD and ISO YYYY-MM-DD[...]."""
    if raw is None or not raw.strip():
        return None, True
    value = raw.strip()
    try:
        if _DATE8_RE.match(value):
            return datetime.strptime(value, "%Y%m%d").date(), True
        if len(value) >= 10 and value[4] == "-" and value[7] == "-":
            return date.fromisoformat(value[:10]), True
    except ValueError:
        pass
    return None, False


def parse_decimal(raw: str | None) -> tuple[Decimal | None, bool]:
    if raw is None or not raw.strip():
        return None, True
    try:
        value = Decimal(raw.strip())
    except InvalidOperation:
        return None, False
    if not value.is_finite() or abs(value) >= _DECIMAL_LIMIT:
        return None, False
    return value, True


def parse_int(raw: str | None) -> tuple[int | None, bool]:
    if raw is None or not raw.strip():
        return None, True
    value = raw.strip()
    if not _INT_RE.match(value):
        return None, False
    return int(value), True


def parse_txn_datetime(raw: str | None) -> tuple[datetime | None, bool]:
    if raw is None or not raw.strip():
        return None, True
    value = raw.strip()
    try:
        if _DATETIME14_RE.match(value):
            return datetime.strptime(value, "%Y%m%d%H%M%S"), True
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None, False
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed, True


def resolve_coverage_year(benefit_effective_date: date | None) -> tuple[int | None, str]:
    if benefit_effective_date is None:
        return None, COVERAGE_YEAR_SOURCE_UNRESOLVED
    return benefit_effective_date.year, COVERAGE_YEAR_SOURCE_BENEFIT


def format_warning_codes(codes: list[str]) -> str | None:
    """Distinct codes in first-seen order, ';'-joined, cut at a code boundary."""
    distinct = list(dict.fromkeys(codes))
    if not distinct:
        return None
    out: list[str] = []
    length = 0
    for code in distinct:
        extra = len(code) + (1 if out else 0)
        if length + extra > WARNING_CODES_MAX - len(W_WARNING_CODES_TRUNCATED) - 1:
            out.append(W_WARNING_CODES_TRUNCATED)
            break
        out.append(code)
        length += extra
    return ";".join(out)


# ------------------------------------------------------------------ attribution
def check_issuer_attribution(
    *, folder_issuer: str | None, filename_issuer: str | None, header_issuer: str | None,
) -> list[str]:
    """
    Folder, filename and header HIOS_Issuer_ID must agree when present (fatal otherwise).
    Returns file-level warnings.
    """
    sources = {
        "folder": folder_issuer,
        "filename": filename_issuer,
        "header HIOS_Issuer_ID": header_issuer,
    }
    present = {k: v.strip() for k, v in sources.items() if v and v.strip()}
    if len(set(present.values())) > 1:
        detail = ", ".join(f"{k}={v}" for k, v in present.items())
        raise OutboundFileFatalError(f"Issuer attribution conflict ({detail})")
    return [] if present.get("header HIOS_Issuer_ID") else [W_HEADER_ISSUER_MISSING]


# --------------------------------------------------------------------- building
@dataclass(frozen=True)
class FileAttribution:
    issuer: str
    folder_year: int
    folder_month: int
    filename_file_year: int | None
    filename_file_month: int | None
    source_file: str
    source_file_path: str
    file_hash: str


@dataclass(frozen=True)
class RunStamp:
    load_run_id: str
    loaded_at: datetime
    parser_version: str
    runner_version: str
    git_commit: str | None


@dataclass
class EnrichedFile:
    rows: list[dict[str, Any]] = field(default_factory=list)
    premium_rows: list[dict[str, Any]] = field(default_factory=list)
    txn_create_datetime: datetime | None = None
    job_execution_id: str | None = None
    file_warnings: list[str] = field(default_factory=list)


def _enrollment_warnings(
    enrollment: OutboundEnrollmentRecord, flags: list[str | None],
) -> list[str]:
    warnings = [f"REPEATED_ELEMENT:enrollment.{k}" for k in enrollment.repeated_fields]
    normalized = [(f or "").strip().upper() for f in flags]
    actual = {
        "qtyt": (len(flags), W_QTYT_MISMATCH),
        "qtyy": (normalized.count("Y"), W_QTYY_MISMATCH),
        "qtyn": (normalized.count("N"), W_QTYN_MISMATCH),
    }
    for key, (count, code) in actual.items():
        value, ok = parse_int(enrollment.fields.get(key))
        if not ok:
            warnings.append(f"INVALID_QTY:{key}")
        elif value is not None and value != count:
            warnings.append(code)
    if normalized.count("Y") > 1:
        warnings.append(W_MULTIPLE_SUBSCRIBERS)
    return warnings


def _check_widths(row: dict[str, Any], limits: dict[str, int], label: str) -> None:
    for col, limit in limits.items():
        value = row.get(col)
        if isinstance(value, str) and len(value) > limit:
            raise OutboundFileFatalError(f"{label}: value for {col} exceeds NVARCHAR({limit})")


def _build_premiums(
    record: OutboundEnrolleeRecord,
    *,
    parent: dict[str, Any],
    policy: SourceJsonPolicy,
    warnings: list[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_periods: set[tuple[int, int]] = set()
    for prem in record.premiums:
        pf = prem.fields
        warnings.extend(f"REPEATED_ELEMENT:premium.{k}" for k in prem.repeated_fields)

        month_start, ok = parse_date(pf.get("month_start_date"))
        if not ok:
            warnings.append("INVALID_PREMIUM_VALUE:month_start_date")
        year_num, ok = parse_int(pf.get("year_num"))
        if not ok:
            warnings.append("INVALID_PREMIUM_VALUE:year_num")
        month_num, ok = parse_int(pf.get("month_num"))
        if not ok or (month_num is not None and not 1 <= month_num <= 12):
            warnings.append("INVALID_PREMIUM_VALUE:month_num")
            month_num = None

        premium_year = year_num if year_num is not None else (month_start.year if month_start else None)
        premium_month = month_num if month_num is not None else (month_start.month if month_start else None)
        if month_start and (
            (year_num is not None and year_num != month_start.year)
            or (month_num is not None and month_num != month_start.month)
        ):
            warnings.append(W_PREMIUM_PERIOD_MISMATCH)
        if premium_year is not None and premium_month is not None:
            period = (premium_year, premium_month)
            if period in seen_periods:
                warnings.append(W_DUPLICATE_PREMIUM_MONTH)
            seen_periods.add(period)

        amounts: dict[str, Decimal | None] = {}
        for name in _PREMIUM_AMOUNT_FIELDS:
            amounts[name], ok = parse_decimal(pf.get(name))
            if not ok:
                warnings.append(f"INVALID_PREMIUM_VALUE:{name}")

        row = {
            "load_run_id": parent["load_run_id"],
            "loaded_at": parent["loaded_at"],
            "file_hash": parent["file_hash"],
            "row_number_in_file": parent["row_number_in_file"],
            "premium_seq": prem.premium_seq,
            "issuer": parent["issuer"],
            "policy_id": parent["policy_id"],
            "member_id": parent["member_id"],
            "enrollment_event_id": parent["enrollment_event_id"],
            "month_start_date": month_start,
            "premium_year": premium_year,
            "premium_month": premium_month,
            **amounts,
            "premium_json": serialize_premium(prem.element, policy),
        }
        _check_widths(
            row, MONTHLY_PREMIUM_MAX_LENGTHS,
            f"row {parent['row_number_in_file']} premium {prem.premium_seq}",
        )
        rows.append(row)
    return rows


def build_outbound_file(
    parsed: OutboundParseResult,
    *,
    attribution: FileAttribution,
    stamp: RunStamp,
    policy: SourceJsonPolicy,
    insurance_map: dict[str, str],
    structure: FileStructure,
    collector: SchemaObservationCollector | None = None,
) -> EnrichedFile:
    result = EnrichedFile()
    result.file_warnings.extend(structure.file_warnings)
    result.file_warnings.extend(f"REPEATED_ELEMENT:header.{k}" for k in parsed.header_repeated)

    txn_dt, ok = parse_txn_datetime(parsed.header.get("txn_create_datetime"))
    if not ok:
        result.file_warnings.append(W_INVALID_TXN_CREATE_DATETIME)
    result.txn_create_datetime = txn_dt
    job_id = parsed.header.get("job_execution_id")
    if job_id is not None and len(job_id) > FILE_LOG_JOB_EXECUTION_ID_MAX:
        raise OutboundFileFatalError(
            f"header jobExecutionId exceeds NVARCHAR({FILE_LOG_JOB_EXECUTION_ID_MAX})"
        )
    result.job_execution_id = job_id

    by_row = {r.row_number_in_file: r for r in parsed.enrollees}
    enrollment_warnings = {
        id(enr): _enrollment_warnings(
            enr, [by_row[n].fields.get("subscriber_flag") for n in enr.row_numbers],
        )
        for enr in parsed.enrollments
    }

    for record in parsed.enrollees:
        rn = record.row_number_in_file
        f = record.fields
        ef = record.enrollment.fields
        warnings: list[str] = list(structure.row_warnings.get(rn, []))
        warnings.extend(enrollment_warnings[id(record.enrollment)])
        warnings.extend(f"REPEATED_ELEMENT:{k}" for k in record.repeated_fields)

        policy_candidates = [
            v for v in (f.get("policy_id"), f.get("health_coverage_policy_id"), ef.get("enrollment_id"))
            if v
        ]
        if not policy_candidates:
            raise OutboundFileFatalError(f"row {rn}: policy identifier missing")
        if len(set(policy_candidates)) > 1:
            warnings.append(W_POLICY_ID_MISMATCH)
        if not f.get("member_id"):
            raise OutboundFileFatalError(f"row {rn}: member identifier (exchgIndivIdentifier) missing")

        if not f.get("enrollment_event_id"):
            warnings.append(W_MISSING_ENROLLMENT_EVENT_ID)
        event_type, maint_type = f.get("enrollee_event_type_code"), f.get("maintenance_type_code")
        if event_type and maint_type and event_type != maint_type:
            warnings.append(W_EVENT_MAINT_TYPE_MISMATCH)
        event_reason, maint_reason = f.get("enrollee_event_reason_code"), f.get("maintenance_reason_code")
        if event_reason and maint_reason and event_reason != maint_reason:
            warnings.append(W_EVENT_MAINT_REASON_MISMATCH)

        plan_id = ef.get("insurer_cms_plan_id")
        class_code = f.get("class_of_contract_code")
        if plan_id and class_code and plan_id != class_code:
            warnings.append(W_CLASS_PLAN_MISMATCH)
        # CMS plan IDs begin with the 5-digit HIOS issuer id.
        if plan_id and not plan_id.startswith(attribution.issuer):
            warnings.append(W_PLAN_ISSUER_MISMATCH)

        txn_type, w = classify_outbound_transaction(event_type, f.get("additional_maint_reason_code"))
        if w:
            warnings.append(w)
        ins_code = f.get("insurance_type_code")
        ins_type, w = map_insurance_type(ins_code, insurance_map)
        if w:
            warnings.append(w)
        if collector is not None:
            collector.observe_code(
                "insurance_type_code", ins_code, attribution.source_file, known=ins_type is not None,
            )
            collector.observe_code(
                "enrollee_event_type_code", event_type, attribution.source_file,
                known=txn_type is not None,
            )

        dates: dict[str, date | None] = {}
        for col in _DATE_COLUMNS:
            dates[col], ok = parse_date(f.get(col))
            if not ok:
                warnings.append(f"INVALID_DATE:{col}")
        coverage_year, coverage_source = resolve_coverage_year(dates["benefit_effective_date"])
        if coverage_year is None:
            warnings.append(W_COVERAGE_YEAR_UNRESOLVED)

        amounts: dict[str, Decimal | None] = {}
        for col in _AMOUNT_COLUMNS:
            amounts[col], ok = parse_decimal(f.get(col))
            if not ok:
                warnings.append(f"INVALID_DECIMAL:{col}")

        source_json, record_hash = serialize_enrollee(record, parsed.root, policy)

        row: dict[str, Any] = {
            "load_run_id": stamp.load_run_id,
            "loaded_at": stamp.loaded_at,
            "issuer": attribution.issuer,
            "folder_year": attribution.folder_year,
            "folder_month": attribution.folder_month,
            "filename_file_year": attribution.filename_file_year,
            "filename_file_month": attribution.filename_file_month,
            "source_file": attribution.source_file,
            "source_file_path": attribution.source_file_path,
            "file_hash": attribution.file_hash,
            "row_number_in_file": rn,
            "raw_record_hash": record_hash,
            "parser_version": stamp.parser_version,
            "runner_version": stamp.runner_version,
            "git_commit": stamp.git_commit,
            **{col: f.get(col) for col in _PASSTHROUGH_COLUMNS},
            "enrollment_action_code": ef.get("enrollment_action_code"),
            "outbound_transaction_type": txn_type,
            "policy_id": policy_candidates[0],
            "subscriber_id": f.get("subscriber_id") or ef.get("enrollment_subscriber_id"),
            "insurance_type": ins_type,
            "insurer_cms_plan_id": plan_id,
            **dates,
            "coverage_year": coverage_year,
            "coverage_year_source": coverage_source,
            **amounts,
            "qtyn": ef.get("qtyn"),
            "qtyy": ef.get("qtyy"),
            "qtyt": ef.get("qtyt"),
            "source_xml_json": source_json,
        }

        premium_rows = _build_premiums(record, parent=row, policy=policy, warnings=warnings)

        row["warning_count"] = len(warnings)
        row["warning_codes"] = format_warning_codes(warnings)
        _check_widths(row, OUTBOUND_AUTOMATION_MAX_LENGTHS, f"row {rn}")
        result.rows.append(row)
        result.premium_rows.extend(premium_rows)

    return result
