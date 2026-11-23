"""
Outbound (TO) 834 XML parser.

Hierarchy:
    enrollments / GS_Info (1..N) / enrollment (1..N) / enrollee (1..N)
    enrollee / memberReportingCategory / monthlyPremiums / premium (0..N)

Every tag is matched by local name, so a namespace URI or prefix never changes
traversal. The parser only extracts raw (stripped) source text; typing, mapping
and validation are done by outbound_automation.enrich. Exact source text is kept
by outbound_automation.source_json from the retained element references.

Separate from parsers/parser_834.py (inbound FROM files), which is not used here.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field

ROOT_TAG = "enrollments"
GS_TAG = "GS_Info"
ENROLLMENT_TAG = "enrollment"
ENROLLEE_TAG = "enrollee"
LOOKUP_CODE_TAG = "lookupValueCode"
PREMIUM_PATH = ("memberReportingCategory", "monthlyPremiums", "premium")

ENROLLEE_PATH = (ROOT_TAG, GS_TAG, ENROLLMENT_TAG, ENROLLEE_TAG)


class OutboundParseError(ValueError):
    """The file cannot be parsed as an outbound TO 834 document."""


def local_name(tag: object) -> str:
    """Local part of an ElementTree tag ('{uri}name' or 'prefix:name' -> 'name')."""
    if not isinstance(tag, str):
        return ""
    if tag.startswith("{"):
        tag = tag.split("}", 1)[1]
    if ":" in tag:
        tag = tag.split(":", 1)[1]
    return tag


def namespace_uri(tag: object) -> str | None:
    if isinstance(tag, str) and tag.startswith("{"):
        return tag[1:].split("}", 1)[0]
    return None


def element_children(el: ET.Element) -> list[ET.Element]:
    return [c for c in el if isinstance(c.tag, str)]


def children_named(el: ET.Element, name: str) -> list[ET.Element]:
    return [c for c in el if local_name(c.tag) == name]


def find_path(el: ET.Element, path: tuple[str, ...]) -> list[ET.Element]:
    current = [el]
    for name in path:
        current = [c for parent in current for c in parent if local_name(c.tag) == name]
        if not current:
            break
    return current


def _stripped_text(el: ET.Element) -> str | None:
    if el.text is None:
        return None
    value = el.text.strip()
    return value or None


@dataclass(frozen=True)
class FieldSpec:
    """
    Canonical key -> candidate relative paths (first path that exists wins).

    lookup=True: value is the <lookupValueCode> child when present, else the
    element's own text.
    """

    key: str
    paths: tuple[tuple[str, ...], ...]
    lookup: bool = False


HEADER_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("hios_issuer_id", (("HIOS_Issuer_ID",),)),
    FieldSpec("txn_create_datetime", (("txnCreateDateTime",),)),
    FieldSpec("job_execution_id", (("jobExecutionId",),)),
)

ENROLLMENT_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("enrollment_id", (("id",),)),
    FieldSpec("enrollment_action_code", (("actionCode",),)),
    FieldSpec("enrollment_subscriber_id", (("exchgSubscriberIdentifier",),)),
    FieldSpec("insurer_cms_plan_id", (("insurerCMSPlanID",),)),
    FieldSpec("qtyn", (("QTYn",),)),
    FieldSpec("qtyy", (("QTYy",),)),
    FieldSpec("qtyt", (("QTYt",),)),
)

ENROLLEE_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("enrollment_event_id", (("enrollmentEventId",),)),
    FieldSpec(
        "enrollee_event_type_code",
        (("enrollmentEvents", "eventTypeLkp"), ("enrollmentEvents", "eventTypeLookUp")),
        lookup=True,
    ),
    FieldSpec(
        "enrollee_event_reason_code",
        (("enrollmentEvents", "eventReasonLkp"), ("enrollmentEvents", "eventReasonLookUp")),
        lookup=True,
    ),
    FieldSpec("maintenance_type_code", (("healthCoverage", "maintenanceTypeCode"),), lookup=True),
    FieldSpec("maintenance_reason_code", (("healthCoverage", "maintenanceReasonCode"),), lookup=True),
    FieldSpec(
        "additional_maint_reason_code",
        (("memberReportingCategory", "additionalMaintReason"),),
        lookup=True,
    ),
    FieldSpec("request_submit_timestamp", (("memberReportingCategory", "requestSubmitTimestamp"),)),
    FieldSpec("payment_txn_id", (("paymentTxnID",),)),
    FieldSpec("policy_id", (("exchgAssignedPolicyID",),)),
    FieldSpec("health_coverage_policy_id", (("healthCoverage", "exchgAssignedPolicyID"),)),
    FieldSpec(
        "previous_policy_id",
        (("memberReportingCategory", "previousExchgAssignedPolicyID"),),
    ),
    FieldSpec("member_id", (("exchgIndivIdentifier",),)),
    FieldSpec("subscriber_id", (("exchgSubscriberIdentifier",),)),
    FieldSpec("issuer_subscriber_identifier", (("issuerSubscriberIdentifier",),)),
    FieldSpec("issuer_indiv_identifier", (("issuerIndivIdentifier",),)),
    FieldSpec("subscriber_flag", (("subscriberFlag",),)),
    FieldSpec("relationship", (("relationshipLkp",),), lookup=True),
    FieldSpec("member_first_name", (("memberFirstName",),)),
    FieldSpec("member_last_name", (("memberLastName",),)),
    FieldSpec("insurance_type_code", (("healthCoverage", "insuranceTypeLkp"),), lookup=True),
    FieldSpec("class_of_contract_code", (("healthCoverage", "classOfContractCode"),)),
    FieldSpec("health_coverage_policy_no", (("healthCoverage", "healthCoveragePolicyNo"),)),
    FieldSpec(
        "household_or_employee_case_id",
        (("healthCoverage", "householdOrEmployeeCaseID"),),
    ),
    FieldSpec("benefit_effective_date", (("healthCoverage", "benefitEffectiveBeginDate"),)),
    FieldSpec("benefit_end_date", (("healthCoverage", "benefitEffectiveEndDate"),)),
    FieldSpec("member_maint_effective_date", (("memberMaintEffectiveDate",),)),
    FieldSpec("last_premium_paid_date", (("healthCoverage", "lastPremiumPaidDate"),)),
    FieldSpec("total_premium_amount", (("memberReportingCategory", "totalPremiumAmt"),)),
    FieldSpec(
        "individual_responsibility_amount",
        (("memberReportingCategory", "totalIndivResponsibilityAmt"),),
    ),
    FieldSpec("aptc_amount", (("memberReportingCategory", "aptcAmt"),)),
    FieldSpec(
        "health_coverage_premium_amount",
        (("memberReportingCategory", "healthCoveragePremiumAmt"),),
    ),
    FieldSpec("rating_area", (("memberReportingCategory", "ratingArea"),)),
    FieldSpec("source_exchg_id", (("memberReportingCategory", "sourceExchgID"),)),
)

PREMIUM_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("month_start_date", (("monthStartDate",),)),
    FieldSpec("year_num", (("yearNum",),)),
    FieldSpec("month_num", (("monthNum",),)),
    FieldSpec("gross_premium_amount", (("grossPremiumAmt",),)),
    FieldSpec("aptc_amount", (("aptcAmt",),)),
    FieldSpec("net_premium_amount", (("netPremiumAmt",),)),
)


def extract_fields(
    el: ET.Element, specs: tuple[FieldSpec, ...],
) -> tuple[dict[str, str | None], list[str]]:
    """Return (values, keys whose single-valued element occurred more than once)."""
    values: dict[str, str | None] = {}
    repeated: list[str] = []
    for spec in specs:
        matches: list[ET.Element] = []
        for path in spec.paths:
            matches = find_path(el, path)
            if matches:
                break
        if not matches:
            values[spec.key] = None
            continue
        if len(matches) > 1:
            repeated.append(spec.key)
        target = matches[0]
        if spec.lookup:
            codes = children_named(target, LOOKUP_CODE_TAG)
            if codes:
                if len(codes) > 1 and spec.key not in repeated:
                    repeated.append(spec.key)
                target = codes[0]
        values[spec.key] = _stripped_text(target)
    return values, repeated


def _absolute(base: tuple[str, ...], specs: tuple[FieldSpec, ...]) -> set[str]:
    out: set[str] = set()
    for spec in specs:
        for path in spec.paths:
            full = "/".join(base + path)
            out.add(full)
            if spec.lookup:
                out.add(f"{full}/{LOOKUP_CODE_TAG}")
    return out


def mapped_paths() -> frozenset[str]:
    """Normalized absolute paths that feed relational columns."""
    paths = _absolute((ROOT_TAG,), HEADER_FIELDS)
    paths |= _absolute((ROOT_TAG, GS_TAG, ENROLLMENT_TAG), ENROLLMENT_FIELDS)
    paths |= _absolute(ENROLLEE_PATH, ENROLLEE_FIELDS)
    paths |= _absolute(ENROLLEE_PATH + PREMIUM_PATH, PREMIUM_FIELDS)
    return frozenset(paths)


@dataclass
class OutboundPremiumRecord:
    premium_seq: int
    fields: dict[str, str | None]
    repeated_fields: list[str]
    element: ET.Element


@dataclass
class OutboundEnrollmentRecord:
    gs_index: int
    enrollment_index: int
    fields: dict[str, str | None]
    repeated_fields: list[str]
    element: ET.Element
    row_numbers: list[int] = field(default_factory=list)


@dataclass
class OutboundEnrolleeRecord:
    row_number_in_file: int
    gs_index: int
    enrollment_index: int
    enrollee_index: int
    fields: dict[str, str | None]
    repeated_fields: list[str]
    premiums: list[OutboundPremiumRecord]
    element: ET.Element
    enrollment: OutboundEnrollmentRecord
    gs_element: ET.Element


@dataclass
class OutboundParseResult:
    root: ET.Element
    header: dict[str, str | None]
    header_repeated: list[str]
    gs_elements: list[ET.Element]
    enrollments: list[OutboundEnrollmentRecord]
    enrollees: list[OutboundEnrolleeRecord]

    @property
    def premium_count(self) -> int:
        return sum(len(e.premiums) for e in self.enrollees)


class Parser834Outbound:
    """Parse one outbound TO 834 XML document into enrollee-grain records."""

    def parse_bytes(self, data: bytes) -> OutboundParseResult:
        try:
            root = ET.fromstring(data)
        except ET.ParseError as exc:
            raise OutboundParseError(f"Malformed XML: {exc}") from exc
        root_name = local_name(root.tag)
        if root_name != ROOT_TAG:
            raise OutboundParseError(
                f"Unexpected root element '{root_name}' (expected '{ROOT_TAG}')"
            )

        header, header_repeated = extract_fields(root, HEADER_FIELDS)
        gs_elements = children_named(root, GS_TAG)
        enrollments: list[OutboundEnrollmentRecord] = []
        enrollees: list[OutboundEnrolleeRecord] = []
        row_number = 0

        for gs_index, gs in enumerate(gs_elements, start=1):
            for enrollment_index, enr in enumerate(children_named(gs, ENROLLMENT_TAG), start=1):
                efields, erepeated = extract_fields(enr, ENROLLMENT_FIELDS)
                enrollment = OutboundEnrollmentRecord(
                    gs_index=gs_index,
                    enrollment_index=enrollment_index,
                    fields=efields,
                    repeated_fields=erepeated,
                    element=enr,
                )
                enrollments.append(enrollment)
                for enrollee_index, ee in enumerate(children_named(enr, ENROLLEE_TAG), start=1):
                    row_number += 1
                    fields, repeated = extract_fields(ee, ENROLLEE_FIELDS)
                    premiums = []
                    for seq, prem in enumerate(find_path(ee, PREMIUM_PATH), start=1):
                        pfields, prepeated = extract_fields(prem, PREMIUM_FIELDS)
                        premiums.append(
                            OutboundPremiumRecord(
                                premium_seq=seq,
                                fields=pfields,
                                repeated_fields=prepeated,
                                element=prem,
                            )
                        )
                    enrollment.row_numbers.append(row_number)
                    enrollees.append(
                        OutboundEnrolleeRecord(
                            row_number_in_file=row_number,
                            gs_index=gs_index,
                            enrollment_index=enrollment_index,
                            enrollee_index=enrollee_index,
                            fields=fields,
                            repeated_fields=repeated,
                            premiums=premiums,
                            element=ee,
                            enrollment=enrollment,
                            gs_element=gs,
                        )
                    )

        return OutboundParseResult(
            root=root,
            header=header,
            header_repeated=header_repeated,
            gs_elements=gs_elements,
            enrollments=enrollments,
            enrollees=enrollees,
        )
