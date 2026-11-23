#!/usr/bin/env python3
"""
Offline tests for the outbound TO 834 automation (no SFTP, no Azure, no real data).

All fixtures are synthetic: issuer 12345, years 2031+, placeholder names.
Database behaviour is exercised with in-memory fakes only.
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from connectors.base_connector import SourceFile  # noqa: E402
from outbound_automation import azure_writer as ow  # noqa: E402
from outbound_automation.config import (  # noqa: E402
    OutboundConfigError,
    load_sensitive_local_names,
    parse_exchange_codes,
    resolve_outbound_remote_path,
    resolve_outbound_source_root,
    resolve_source_json_policy,
)
from outbound_automation.constants import OUTBOUND_TABLES  # noqa: E402
from outbound_automation.enrich import (  # noqa: E402
    FileAttribution,
    OutboundFileFatalError,
    RunStamp,
    build_outbound_file,
    check_issuer_attribution,
    format_warning_codes,
)
from outbound_automation.file_rules import (  # noqa: E402
    classify_outbound_filename,
    is_outbound_to_file,
    parse_to_filename,
)
from outbound_automation.integrity import (  # noqa: E402
    ParentChildIntegrityError,
    validate_parent_child,
)
from outbound_automation.mappings import (  # noqa: E402
    classify_outbound_transaction,
    load_insurance_type_map,
    map_insurance_type,
)
from outbound_automation.pipeline import run_dry_run, run_load  # noqa: E402
from outbound_automation.reports import (  # noqa: E402
    REPORT_ROW_KEYS,
    build_file_summary,
    build_transaction_summary,
    build_warning_summary,
)
from outbound_automation.run_context import OutboundRunContext, new_load_run_id  # noqa: E402
from outbound_automation.schema_observation import (  # noqa: E402
    KnownPathRegistry,
    SchemaObservationCollector,
)
from outbound_automation.schema_v2 import (  # noqa: E402
    MONTHLY_PREMIUM_COLUMNS,
    OUTBOUND_AUTOMATION_COLUMNS,
)
from outbound_automation.source_json import SourceJsonPolicy  # noqa: E402
from parsers.parser_834_outbound import (  # noqa: E402
    OutboundParseError,
    Parser834Outbound,
    mapped_paths,
)

V2_DDL = ROOT / "sql" / "outbound_automation_ddl_v2.sql"
OUTBOUND_PKG = ROOT / "outbound_automation"
OUTBOUND_PARSER = ROOT / "parsers" / "parser_834_outbound.py"

ISSUER = "12345"
PLAN = f"{ISSUER}XX0010001-01"
FULL = SourceJsonPolicy(name="full", redact_local_names=frozenset())
REDACT = SourceJsonPolicy(name="redact", redact_local_names=load_sensitive_local_names())

PII_FIRST = "Zelphina"
PII_SSN = "900112233"


# =============================================================================
# Synthetic XML builders
# =============================================================================
def premium_xml(year: int, month: int, *, gross="300.00", aptc="200.00", net="100.00", extra="") -> str:
    return (
        f"<premium><monthStartDate>{year}{month:02d}01</monthStartDate>"
        f"<yearNum>{year}</yearNum><monthNum>{month}</monthNum>"
        f"<grossPremiumAmt>{gross}</grossPremiumAmt><aptcAmt>{aptc}</aptcAmt>"
        f"<netPremiumAmt>{net}</netPremiumAmt>{extra}</premium>"
    )


def enrollee_xml(
    *,
    member: str | None = "MBR0001",
    policy: str | None = "POL0001",
    subscriber: str = "Y",
    event: str = "021",
    reason: str = "AI",
    maint: str | None = None,
    add_reason: str | None = None,
    ins: str = "HLT",
    begin: str | None = "20310101",
    event_id: str | None = "EVT0001",
    premiums: list[str] | None = None,
    reason_tag: str = "eventReasonLkp",
    class_code: str = PLAN,
    first: str = PII_FIRST,
    ssn: str = PII_SSN,
    extra: str = "",
    attrs: str = "",
) -> str:
    maint = maint or event
    parts = [f"<enrollee{attrs}>"]
    if event_id is not None:
        parts.append(f"<enrollmentEventId>{event_id}</enrollmentEventId>")
    parts.append(
        "<enrollmentEvents>"
        f"<eventTypeLkp><lookupValueCode>{event}</lookupValueCode></eventTypeLkp>"
        f"<{reason_tag}><lookupValueCode>{reason}</lookupValueCode></{reason_tag}>"
        "</enrollmentEvents>"
    )
    if policy is not None:
        parts.append(f"<exchgAssignedPolicyID>{policy}</exchgAssignedPolicyID>")
    if member is not None:
        parts.append(f"<exchgIndivIdentifier>{member}</exchgIndivIdentifier>")
    parts.append(f"<subscriberFlag>{subscriber}</subscriberFlag>")
    parts.append(f"<memberFirstName>{first}</memberFirstName>")
    parts.append(f"<memberSSN>{ssn}</memberSSN>")
    parts.append("<relationshipLkp><lookupValueCode>18</lookupValueCode></relationshipLkp>")
    parts.append("<healthCoverage>")
    if begin is not None:
        parts.append(f"<benefitEffectiveBeginDate>{begin}</benefitEffectiveBeginDate>")
    parts.append(
        f"<insuranceTypeLkp><lookupValueCode>{ins}</lookupValueCode></insuranceTypeLkp>"
        f"<maintenanceTypeCode><lookupValueCode>{maint}</lookupValueCode></maintenanceTypeCode>"
        f"<maintenanceReasonCode><lookupValueCode>{reason}</lookupValueCode></maintenanceReasonCode>"
        f"<classOfContractCode>{class_code}</classOfContractCode>"
        "</healthCoverage>"
    )
    parts.append("<memberReportingCategory>")
    if add_reason:
        parts.append(
            f"<additionalMaintReason><lookupValueCode>{add_reason}</lookupValueCode>"
            "</additionalMaintReason>"
        )
    parts.append("<requestSubmitTimestamp>2031010112000000</requestSubmitTimestamp>")
    if premiums is not None:
        parts.append(f"<monthlyPremiums>{''.join(premiums)}</monthlyPremiums>")
    parts.append("</memberReportingCategory>")
    parts.append(extra)
    parts.append("</enrollee>")
    return "".join(parts)


def enrollment_xml(
    enrollees: list[str],
    *,
    enrollment_id: str | None = "POL0001",
    plan: str = PLAN,
    qty: tuple[str, str, str] | None = None,
) -> str:
    parts = ["<enrollment>"]
    if enrollment_id is not None:
        parts.append(f"<id>{enrollment_id}</id>")
    parts.append("<actionCode>2</actionCode>")
    if qty is not None:
        qtyt, qtyy, qtyn = qty
        parts.append(f"<QTYt>{qtyt}</QTYt><QTYy>{qtyy}</QTYy><QTYn>{qtyn}</QTYn>")
    parts.append(f"<insurerCMSPlanID>{plan}</insurerCMSPlanID>")
    parts.extend(enrollees)
    parts.append("</enrollment>")
    return "".join(parts)


def doc_xml(
    gs_blocks: list[list[str]],
    *,
    header_issuer: str | None = ISSUER,
    root_open: str = "<enrollments>",
    root_close: str = "</enrollments>",
) -> bytes:
    parts = ['<?xml version="1.0" encoding="UTF-8"?>', root_open]
    if header_issuer is not None:
        parts.append(f"<HIOS_Issuer_ID>{header_issuer}</HIOS_Issuer_ID>")
    parts.append("<txnCreateDateTime>20310102030405</txnCreateDateTime>")
    parts.append("<jobExecutionId>JOB-1</jobExecutionId>")
    for enrollments in gs_blocks:
        parts.append("<GS_Info><GS02>S</GS02><GS03>R</GS03>")
        parts.extend(enrollments)
        parts.append("</GS_Info>")
    parts.append(root_close)
    return "".join(parts).encode("utf-8")


def simple_doc(**enrollee_kwargs) -> bytes:
    return doc_xml([[enrollment_xml([enrollee_xml(**enrollee_kwargs)])]])


def attribution(file_hash: str = "h" * 64, folder_year: int = 2099) -> FileAttribution:
    return FileAttribution(
        issuer=ISSUER,
        folder_year=folder_year,
        folder_month=7,
        filename_file_year=2099,
        filename_file_month=7,
        source_file="to_12345_GA_834_INDV_20990701000000.xml",
        source_file_path="/synthetic/to_12345_GA_834_INDV_20990701000000.xml",
        file_hash=file_hash,
    )


STAMP = RunStamp(
    load_run_id="outbound_test_run",
    loaded_at=datetime(2031, 1, 2, tzinfo=timezone.utc),
    parser_version="test",
    runner_version="test",
    git_commit=None,
)


def new_collector() -> SchemaObservationCollector:
    return SchemaObservationCollector(
        KnownPathRegistry.load(), mapped_paths(), load_sensitive_local_names(),
    )


def enrich(data: bytes, *, policy: SourceJsonPolicy = FULL, collector=None, attr=None):
    parsed = Parser834Outbound().parse_bytes(data)
    collector = collector or new_collector()
    structure = collector.observe_file("synthetic.xml", parsed)
    return build_outbound_file(
        parsed,
        attribution=attr or attribution(),
        stamp=STAMP,
        policy=policy,
        insurance_map=load_insurance_type_map(),
        structure=structure,
        collector=collector,
    )


def codes(row: dict) -> list[str]:
    return (row.get("warning_codes") or "").split(";")


# =============================================================================
# Parser traversal
# =============================================================================
class ParserTraversalTests(unittest.TestCase):
    def test_gs_info_traversal_and_row_numbers(self) -> None:
        data = doc_xml([[enrollment_xml([
            enrollee_xml(member="M1"), enrollee_xml(member="M2", subscriber="N"),
        ])]])
        parsed = Parser834Outbound().parse_bytes(data)
        self.assertEqual([r.row_number_in_file for r in parsed.enrollees], [1, 2])
        self.assertEqual([r.fields["member_id"] for r in parsed.enrollees], ["M1", "M2"])

    def test_multiple_gs_info_continue_numbering(self) -> None:
        data = doc_xml([
            [enrollment_xml([enrollee_xml(member="A1")])],
            [enrollment_xml([enrollee_xml(member="B1"), enrollee_xml(member="B2")])],
        ])
        parsed = Parser834Outbound().parse_bytes(data)
        self.assertEqual(len(parsed.gs_elements), 2)
        self.assertEqual([r.row_number_in_file for r in parsed.enrollees], [1, 2, 3])
        self.assertEqual([r.gs_index for r in parsed.enrollees], [1, 2, 2])
        self.assertEqual([r.fields["member_id"] for r in parsed.enrollees], ["A1", "B1", "B2"])

    def test_multiple_enrollments_and_enrollees(self) -> None:
        data = doc_xml([[
            enrollment_xml([enrollee_xml(member="M1"), enrollee_xml(member="M2")], enrollment_id="P1"),
            enrollment_xml([enrollee_xml(member="M3", policy="P2")], enrollment_id="P2"),
        ]])
        parsed = Parser834Outbound().parse_bytes(data)
        self.assertEqual(len(parsed.enrollments), 2)
        self.assertEqual(len(parsed.enrollees), 3)
        self.assertEqual(parsed.enrollments[0].row_numbers, [1, 2])
        self.assertEqual(parsed.enrollments[1].row_numbers, [3])
        self.assertEqual([r.enrollee_index for r in parsed.enrollees], [1, 2, 1])

    def test_default_namespace_local_names(self) -> None:
        data = doc_xml(
            [[enrollment_xml([enrollee_xml(member="NS1")])]],
            root_open='<enrollments xmlns="urn:synthetic:834">',
        )
        parsed = Parser834Outbound().parse_bytes(data)
        self.assertEqual(parsed.enrollees[0].fields["member_id"], "NS1")
        self.assertEqual(parsed.enrollees[0].fields["enrollee_event_type_code"], "021")
        self.assertEqual(parsed.header["hios_issuer_id"], ISSUER)

    def test_prefixed_namespace_local_names(self) -> None:
        body = (
            '<t:enrollments xmlns:t="urn:synthetic:pfx"><t:HIOS_Issuer_ID>12345</t:HIOS_Issuer_ID>'
            "<t:GS_Info><t:enrollment><t:id>P9</t:id><t:enrollee>"
            "<t:exchgIndivIdentifier>PFX1</t:exchgIndivIdentifier>"
            "<t:exchgAssignedPolicyID>P9</t:exchgAssignedPolicyID>"
            "</t:enrollee></t:enrollment></t:GS_Info></t:enrollments>"
        ).encode()
        parsed = Parser834Outbound().parse_bytes(body)
        self.assertEqual(parsed.enrollees[0].fields["member_id"], "PFX1")
        self.assertEqual(parsed.enrollees[0].fields["policy_id"], "P9")

    def test_optional_elements_absent(self) -> None:
        parsed = Parser834Outbound().parse_bytes(simple_doc(event_id=None, premiums=None, begin=None))
        rec = parsed.enrollees[0]
        self.assertIsNone(rec.fields["enrollment_event_id"])
        self.assertIsNone(rec.fields["benefit_effective_date"])
        self.assertIsNone(rec.fields["previous_policy_id"])
        self.assertEqual(rec.premiums, [])

    def test_event_reason_tag_variant(self) -> None:
        for tag in ("eventReasonLkp", "eventReasonLookUp"):
            parsed = Parser834Outbound().parse_bytes(simple_doc(reason_tag=tag, reason="07"))
            self.assertEqual(parsed.enrollees[0].fields["enrollee_event_reason_code"], "07", tag)

    def test_repeated_single_element_flagged(self) -> None:
        data = simple_doc(extra="<paymentTxnID>X1</paymentTxnID><paymentTxnID>X2</paymentTxnID>")
        rec = Parser834Outbound().parse_bytes(data).enrollees[0]
        self.assertEqual(rec.fields["payment_txn_id"], "X1")
        self.assertIn("payment_txn_id", rec.repeated_fields)

    def test_malformed_xml_raises(self) -> None:
        with self.assertRaises(OutboundParseError):
            Parser834Outbound().parse_bytes(b"<enrollments><GS_Info></enrollments>")

    def test_wrong_root_raises(self) -> None:
        with self.assertRaises(OutboundParseError):
            Parser834Outbound().parse_bytes(b"<root><enrollment/></root>")


# =============================================================================
# Enrichment / mapping / validation
# =============================================================================
class InsuranceTypeTests(unittest.TestCase):
    def test_dental_mapping(self) -> None:
        row = enrich(simple_doc(ins="DEN")).rows[0]
        self.assertEqual(row["insurance_type_code"], "DEN")
        self.assertEqual(row["insurance_type"], "Dental")

    def test_unknown_code_is_null_with_warning(self) -> None:
        collector = new_collector()
        row = enrich(simple_doc(ins="QQZ"), collector=collector).rows[0]
        self.assertEqual(row["insurance_type_code"], "QQZ")
        self.assertIsNone(row["insurance_type"])
        self.assertIn("UNKNOWN_INSURANCE_TYPE", codes(row))
        frame = collector.codes_frame()
        hit = frame[(frame["category"] == "insurance_type_code") & (frame["code"] == "QQZ")]
        self.assertEqual(len(hit), 1)
        self.assertFalse(bool(hit.iloc[0]["known"]))

    def test_map_function_never_defaults(self) -> None:
        self.assertEqual(map_insurance_type("ABC", {"DEN": "Dental"}), (None, "UNKNOWN_INSURANCE_TYPE"))
        self.assertEqual(map_insurance_type(None, {"DEN": "Dental"})[0], None)


class CoverageYearTests(unittest.TestCase):
    def test_from_benefit_effective_begin_date_not_folder(self) -> None:
        row = enrich(simple_doc(begin="20330315"), attr=attribution(folder_year=2099)).rows[0]
        self.assertEqual(row["coverage_year"], 2033)
        self.assertEqual(row["coverage_year_source"], "benefit_effective_begin_date")
        self.assertEqual(row["folder_year"], 2099)

    def test_unresolved_when_missing(self) -> None:
        row = enrich(simple_doc(begin=None)).rows[0]
        self.assertIsNone(row["coverage_year"])
        self.assertEqual(row["coverage_year_source"], "unresolved")
        self.assertIn("COVERAGE_YEAR_UNRESOLVED", codes(row))

    def test_unresolved_when_invalid(self) -> None:
        row = enrich(simple_doc(begin="20331345")).rows[0]
        self.assertIsNone(row["coverage_year"])
        self.assertIn("INVALID_DATE:benefit_effective_date", codes(row))
        self.assertIn("COVERAGE_YEAR_UNRESOLVED", codes(row))


class TransactionTypeTests(unittest.TestCase):
    def test_mapping_table(self) -> None:
        cases = [
            (("021", None), ("ADD", None)),
            (("001", None), ("CHANGE", None)),
            (("024", "CANCEL"), ("CANCEL", None)),
            (("024", "term"), ("TERM", None)),
            (("024", None), ("CANCEL_OR_TERM", None)),
            (("024", "OTHER"), ("CANCEL_OR_TERM", None)),
            (("025", None), ("REINSTATE", None)),
            (("999", None), (None, "UNKNOWN_TRANSACTION_TYPE")),
            ((None, None), (None, "UNKNOWN_TRANSACTION_TYPE")),
        ]
        for args, expected in cases:
            self.assertEqual(classify_outbound_transaction(*args), expected, args)

    def test_row_keeps_raw_codes(self) -> None:
        row = enrich(simple_doc(event="024", add_reason="TERM", reason="07")).rows[0]
        self.assertEqual(row["outbound_transaction_type"], "TERM")
        self.assertEqual(row["enrollee_event_type_code"], "024")
        self.assertEqual(row["maintenance_type_code"], "024")
        self.assertEqual(row["additional_maint_reason_code"], "TERM")


class ValidationTests(unittest.TestCase):
    def test_qty_mismatches(self) -> None:
        data = doc_xml([[enrollment_xml(
            [enrollee_xml(member="M1"), enrollee_xml(member="M2", subscriber="N")],
            qty=("3", "1", "2"),
        )]])
        row = enrich(data).rows[0]
        self.assertIn("QTYT_MISMATCH", codes(row))
        self.assertIn("QTYN_MISMATCH", codes(row))
        self.assertNotIn("QTYY_MISMATCH", codes(row))

    def test_qty_consistent_no_warning(self) -> None:
        data = doc_xml([[enrollment_xml(
            [enrollee_xml(member="M1"), enrollee_xml(member="M2", subscriber="N")],
            qty=("2", "1", "1"),
        )]])
        for row in enrich(data).rows:
            self.assertFalse(any(c.startswith("QTY") for c in codes(row)))

    def test_multiple_subscribers_warning(self) -> None:
        data = doc_xml([[enrollment_xml([enrollee_xml(member="M1"), enrollee_xml(member="M2")])]])
        self.assertIn("MULTIPLE_SUBSCRIBERS_IN_ENROLLMENT", codes(enrich(data).rows[0]))

    def test_policy_identifier_disagreement(self) -> None:
        data = doc_xml([[enrollment_xml([enrollee_xml(policy="PA")], enrollment_id="PB")]])
        row = enrich(data).rows[0]
        self.assertEqual(row["policy_id"], "PA")
        self.assertIn("POLICY_ID_MISMATCH", codes(row))

    def test_event_vs_maintenance_mismatch(self) -> None:
        row = enrich(simple_doc(event="001", maint="024")).rows[0]
        self.assertIn("EVENT_MAINT_TYPE_MISMATCH", codes(row))

    def test_class_of_contract_vs_plan_mismatch(self) -> None:
        row = enrich(simple_doc(class_code=f"{ISSUER}XX0010002-01")).rows[0]
        self.assertIn("CLASS_PLAN_MISMATCH", codes(row))

    def test_missing_enrollment_event_id(self) -> None:
        row = enrich(simple_doc(event_id=None)).rows[0]
        self.assertIsNone(row["enrollment_event_id"])
        self.assertIn("MISSING_ENROLLMENT_EVENT_ID", codes(row))

    def test_invalid_decimal(self) -> None:
        data = simple_doc(premiums=[premium_xml(2031, 1, gross="abc")])
        self.assertIn("INVALID_PREMIUM_VALUE:gross_premium_amount", codes(enrich(data).rows[0]))

    def test_missing_member_is_fatal(self) -> None:
        with self.assertRaises(OutboundFileFatalError):
            enrich(simple_doc(member=None))

    def test_missing_policy_is_fatal(self) -> None:
        data = doc_xml([[enrollment_xml([enrollee_xml(policy=None)], enrollment_id=None)]])
        with self.assertRaises(OutboundFileFatalError):
            enrich(data)

    def test_value_wider_than_column_is_fatal(self) -> None:
        with self.assertRaises(OutboundFileFatalError) as ctx:
            enrich(simple_doc(member="M" * 101))
        self.assertNotIn("M" * 101, str(ctx.exception))

    def test_issuer_attribution(self) -> None:
        self.assertEqual(
            check_issuer_attribution(folder_issuer=ISSUER, filename_issuer=ISSUER, header_issuer=ISSUER),
            [],
        )
        self.assertEqual(
            check_issuer_attribution(folder_issuer=ISSUER, filename_issuer=ISSUER, header_issuer=None),
            ["HEADER_ISSUER_MISSING"],
        )
        with self.assertRaises(OutboundFileFatalError):
            check_issuer_attribution(folder_issuer=ISSUER, filename_issuer=ISSUER, header_issuer="54321")

    def test_warning_count_and_codes(self) -> None:
        row = enrich(simple_doc(event_id=None, begin=None)).rows[0]
        self.assertGreaterEqual(row["warning_count"], 2)
        self.assertEqual(len(codes(row)), len(set(codes(row))))

    def test_warning_codes_truncated_at_boundary(self) -> None:
        out = format_warning_codes([f"CODE_{i:04d}" for i in range(500)])
        self.assertLessEqual(len(out), 2000)
        self.assertTrue(out.endswith("WARNING_CODES_TRUNCATED"))

    def test_warnings_contain_no_values(self) -> None:
        row = enrich(simple_doc(event_id=None, begin="bad", ins="QQZ")).rows[0]
        for value in (PII_FIRST, PII_SSN, "MBR0001", "POL0001", "bad"):
            self.assertNotIn(value, row["warning_codes"])


# =============================================================================
# Monthly premiums
# =============================================================================
class MonthlyPremiumTests(unittest.TestCase):
    def test_zero_premiums(self) -> None:
        result = enrich(simple_doc(premiums=None))
        self.assertEqual(result.premium_rows, [])
        result = enrich(simple_doc(premiums=[]))
        self.assertEqual(result.premium_rows, [])

    def test_variable_non_consecutive_premiums(self) -> None:
        prems = [premium_xml(2031, 1), premium_xml(2031, 5), premium_xml(2032, 11)]
        result = enrich(simple_doc(premiums=prems))
        self.assertEqual([p["premium_seq"] for p in result.premium_rows], [1, 2, 3])
        self.assertEqual(
            [(p["premium_year"], p["premium_month"]) for p in result.premium_rows],
            [(2031, 1), (2031, 5), (2032, 11)],
        )
        self.assertNotIn("DUPLICATE_PREMIUM_MONTH", codes(result.rows[0]))
        parent = result.rows[0]
        for p in result.premium_rows:
            self.assertEqual(p["file_hash"], parent["file_hash"])
            self.assertEqual(p["row_number_in_file"], parent["row_number_in_file"])
            self.assertEqual(p["policy_id"], parent["policy_id"])

    def test_repeated_month_kept_with_warning(self) -> None:
        prems = [premium_xml(2031, 3), premium_xml(2031, 3, gross="1.00")]
        result = enrich(simple_doc(premiums=prems))
        self.assertEqual(len(result.premium_rows), 2)
        self.assertIn("DUPLICATE_PREMIUM_MONTH", codes(result.rows[0]))

    def test_unknown_premium_tag_retained(self) -> None:
        prems = [premium_xml(2031, 1, extra="<stateSubsidyAmt>12.34</stateSubsidyAmt>")]
        collector = new_collector()
        result = enrich(simple_doc(premiums=prems), collector=collector)
        doc = json.loads(result.premium_rows[0]["premium_json"])
        tags = [c["tag"] for c in doc["premium"]["children"]]
        self.assertIn("stateSubsidyAmt", tags)
        self.assertIn("12.34", result.premium_rows[0]["premium_json"])
        self.assertIn("UNKNOWN_XML_PATH", codes(result.rows[0]))
        frame = collector.paths_frame()
        unknown = frame[~frame["known"]]
        self.assertTrue(unknown["normalized_path"].str.endswith("premium/stateSubsidyAmt").any())


# =============================================================================
# source_xml_json
# =============================================================================
class SourceJsonTests(unittest.TestCase):
    def test_structure_and_context(self) -> None:
        row = enrich(simple_doc(premiums=[premium_xml(2031, 1)])).rows[0]
        doc = json.loads(row["source_xml_json"])
        self.assertEqual(doc["format"], "outbound_834_source_json_v1")
        self.assertEqual(doc["root"]["tag"], "enrollments")
        self.assertEqual(doc["root"]["omitted_children"], {"GS_Info": 1})
        self.assertEqual(doc["gs_info"]["omitted_children"], {"enrollment": 1})
        self.assertEqual(doc["enrollment"]["omitted_children"], {"enrollee": 1})
        self.assertEqual(doc["enrollee"]["tag"], "enrollee")
        self.assertEqual(doc["position"]["row_number_in_file"], 1)
        header_tags = [c["tag"] for c in doc["root"]["children"]]
        self.assertEqual(header_tags[:3], ["HIOS_Issuer_ID", "txnCreateDateTime", "jobExecutionId"])

    def test_unknown_path_attribute_and_exact_text_preserved(self) -> None:
        extra = '<futureTag kind="x">  spaced value  </futureTag><futureTag>second</futureTag>'
        collector = new_collector()
        row = enrich(simple_doc(extra=extra, attrs=' seq="7"'), collector=collector).rows[0]
        doc = json.loads(row["source_xml_json"])
        enrollee = doc["enrollee"]
        self.assertEqual(enrollee["attrs"], {"seq": "7"})
        future = [c for c in enrollee["children"] if c["tag"] == "futureTag"]
        self.assertEqual(len(future), 2)
        self.assertEqual(future[0]["text"], "  spaced value  ")
        self.assertEqual(future[0]["attrs"], {"kind": "x"})
        self.assertIn("UNKNOWN_XML_PATH", codes(row))
        self.assertIn("UNKNOWN_XML_ATTRIBUTE", codes(row))

    def test_namespace_preserved(self) -> None:
        data = doc_xml(
            [[enrollment_xml([enrollee_xml()])]],
            root_open='<enrollments xmlns="urn:synthetic:834">',
        )
        row = enrich(data).rows[0]
        doc = json.loads(row["source_xml_json"])
        self.assertEqual(doc["enrollee"]["ns"], "urn:synthetic:834")
        self.assertIn("XML_NAMESPACE_PRESENT", codes(row))

    def test_redact_policy(self) -> None:
        full = enrich(simple_doc(), policy=FULL).rows[0]
        red = enrich(simple_doc(), policy=REDACT).rows[0]
        self.assertIn(PII_SSN, full["source_xml_json"])
        self.assertNotIn(PII_SSN, red["source_xml_json"])
        self.assertNotIn(PII_FIRST, red["source_xml_json"])
        self.assertIn("***REDACTED***", red["source_xml_json"])
        self.assertEqual(full["raw_record_hash"], red["raw_record_hash"])

    def test_policy_resolution_has_no_default_for_load(self) -> None:
        fake = SimpleNamespace(refresh_from_env=lambda: None)
        with patch("outbound_automation.config.settings", fake), \
             patch.dict(os.environ, {"OUTBOUND_SOURCE_JSON_PII_POLICY": ""}, clear=False):
            with self.assertRaises(OutboundConfigError):
                resolve_source_json_policy(required=True)
            dry = resolve_source_json_policy(required=False)
            self.assertTrue(dry.redact_local_names)
        with patch("outbound_automation.config.settings", fake), \
             patch.dict(os.environ, {"OUTBOUND_SOURCE_JSON_PII_POLICY": "bogus"}, clear=False):
            with self.assertRaises(OutboundConfigError):
                resolve_source_json_policy(required=True)
        with patch("outbound_automation.config.settings", fake), \
             patch.dict(os.environ, {"OUTBOUND_SOURCE_JSON_PII_POLICY": "full"}, clear=False):
            self.assertEqual(resolve_source_json_policy(required=True).redact_local_names, frozenset())


# =============================================================================
# Schema observation
# =============================================================================
class SchemaObservationTests(unittest.TestCase):
    def test_registry_is_structure_only(self) -> None:
        registry = KnownPathRegistry.load()
        self.assertTrue(registry.paths)
        for path in registry.paths:
            self.assertTrue(path.startswith("enrollments"), path)
            self.assertRegex(path, r"^[A-Za-z0-9_/]+$")

    def test_mapped_paths_are_known(self) -> None:
        registry = KnownPathRegistry.load()
        self.assertEqual(set(), set(mapped_paths()) - set(registry.paths))

    def test_frames_never_contain_values(self) -> None:
        collector = new_collector()
        enrich(simple_doc(extra=f"<futureName>{PII_FIRST}</futureName>"), collector=collector)
        for frame in (collector.paths_frame(), collector.codes_frame(),
                      collector.attributes_namespaces_frame()):
            text = frame.astype(str).to_string()
            for value in (PII_FIRST, PII_SSN, "MBR0001", "POL0001"):
                self.assertNotIn(value, text)
        paths = collector.paths_frame().set_index("normalized_path")
        ssn_path = "enrollments/GS_Info/enrollment/enrollee/memberSSN"
        self.assertIn("<suppressed>", paths.loc[ssn_path, "value_shapes"])


# =============================================================================
# Filename classification
# =============================================================================
class FileRuleTests(unittest.TestCase):
    GA = frozenset({"GA"})

    def test_standard_name_parsed_dynamically(self) -> None:
        name = parse_to_filename("to_54321_GA_834_INDV_20310203040506.xml", exchange_codes=self.GA)
        self.assertEqual((name.issuer, name.exchange, name.timestamp), ("54321", "GA", "20310203040506"))
        self.assertEqual((name.file_year, name.file_month), (2031, 2))

    def test_extensions(self) -> None:
        base = "to_12345_GA_834_INDV_20310101000000.xml"
        self.assertEqual(classify_outbound_filename(base, exchange_codes=self.GA), "valid_xml")
        self.assertEqual(classify_outbound_filename(base + ".gz", exchange_codes=self.GA), "valid_gz")
        self.assertEqual(classify_outbound_filename(base + ".xz", exchange_codes=self.GA), "valid_xz")

    def test_exchange_code_configurable(self) -> None:
        name = "to_12345_ZZ_834_INDV_20310101000000.xml"
        self.assertEqual(classify_outbound_filename(name, exchange_codes=self.GA), "to_nonstandard")
        self.assertTrue(is_outbound_to_file(name, exchange_codes=frozenset({"ZZ"})))
        self.assertEqual(parse_exchange_codes("zz, GA"), frozenset({"ZZ", "GA"}))
        self.assertEqual(parse_exchange_codes(""), frozenset({"GA"}))

    def test_nonstandard_to_reported_not_selected(self) -> None:
        for name in ("to_12345_x.xml", "to_12345_GA_834_SHOP_20310101000000.xml",
                     "to_1234_GA_834_INDV_20310101000000.xml"):
            self.assertEqual(classify_outbound_filename(name, exchange_codes=self.GA), "to_nonstandard", name)
            self.assertFalse(is_outbound_to_file(name, exchange_codes=self.GA), name)

    def test_from_and_non_834_rejected(self) -> None:
        self.assertEqual(
            classify_outbound_filename("from_12345_GA_834_INDV_20310101000000.xml", exchange_codes=self.GA),
            "from",
        )
        for name in ("to_12345.edi", "to_12345_report.xml", "to_12345_tracking.xml",
                     "to_12345_summary.xml", "to_12345.txt", "to_12345.dtl"):
            self.assertFalse(is_outbound_to_file(name, exchange_codes=self.GA), name)


# =============================================================================
# Writer contract (V2 DDL)
# =============================================================================
def _ddl_columns(table: str) -> set[str]:
    ddl = V2_DDL.read_text(encoding="utf-8")
    block = ddl.split(f"CREATE TABLE dbo.{table} (", 1)[1].split("CONSTRAINT PK_", 1)[0]
    cols = {
        m.group(1)
        for m in re.finditer(
            r"^\s{4}(\w+)\s+(?:BIGINT|NVARCHAR|INT|DATETIME2|DATE|DECIMAL)", block, re.MULTILINE,
        )
    }
    cols.discard("id")
    return cols


class WriterContractTests(unittest.TestCase):
    def test_main_columns_match_v2_ddl(self) -> None:
        self.assertEqual(_ddl_columns("outbound_automation"), set(OUTBOUND_AUTOMATION_COLUMNS))

    def test_premium_columns_match_v2_ddl(self) -> None:
        self.assertEqual(
            _ddl_columns("outbound_automation_monthly_premium"), set(MONTHLY_PREMIUM_COLUMNS),
        )

    def test_log_columns_match_v2_ddl(self) -> None:
        self.assertEqual(_ddl_columns("outbound_automation_run_log"), set(ow.RUN_LOG_COLUMNS))
        self.assertEqual(_ddl_columns("outbound_automation_file_log"), set(ow.FILE_LOG_COLUMNS))

    def test_removed_columns_not_resurrected(self) -> None:
        removed = {
            "coverage_status", "action_code_description", "enrolleeStatus", "action_code",
            "user_fee_amount", "raw_payload", "raw_json", "year", "month", "file_name",
            "raw_xml_path", "created_at", "insurer_tax_id_number", "exchg_assigned_enrollee_id",
        }
        self.assertEqual(removed & set(OUTBOUND_AUTOMATION_COLUMNS), set())

    def test_enriched_row_covers_writer_columns(self) -> None:
        result = enrich(simple_doc(premiums=[premium_xml(2031, 1)]))
        self.assertEqual(set(OUTBOUND_AUTOMATION_COLUMNS) - set(result.rows[0]), set())
        self.assertEqual(set(MONTHLY_PREMIUM_COLUMNS) - set(result.premium_rows[0]), set())

    def test_sql_targets_only_outbound(self) -> None:
        sqls = [
            ow._RUN_LOG_INSERT_SQL, ow._RUN_LOG_UPDATE_SQL, ow._FILE_LOG_INSERT_SQL,
            ow._FILE_LOG_UPDATE_SQL, ow._FILE_LOG_EXISTS_SQL, ow._row_insert_sql(),
            ow._premium_insert_sql(),
        ]
        for sql in map(str, sqls):
            self.assertNotIn("inbound", sql.lower())
            targets = set(re.findall(r"\[dbo\]\.\[(\w+)\]", sql))
            self.assertTrue(targets)
            self.assertTrue(targets <= set(OUTBOUND_TABLES), targets)

    def test_allowlist_rejects_inbound(self) -> None:
        for table in ("inbound_automation", "inbound_automation_run_log",
                      "inbound_automation_file_log"):
            with self.assertRaises(RuntimeError):
                ow._assert_outbound_target(table)

    def test_v2_ddl_has_no_foreign_key(self) -> None:
        executable = "\n".join(
            line for line in V2_DDL.read_text(encoding="utf-8").splitlines()
            if not line.strip().startswith("--")
        )
        self.assertNotIn("FOREIGN KEY", executable.upper())
        self.assertNotIn("REFERENCES", executable.upper())

    def test_sanitize_db_error_removes_values(self) -> None:
        class FakeDBError(Exception):
            def __init__(self) -> None:
                super().__init__("wrapper")
                self.orig = Exception(
                    "String or binary data would be truncated in table 'dbo.outbound_automation', "
                    "column 'member_first_name'. Truncated value: 'Zelph.ina O'Neil'. "
                    "[SQL: INSERT ...] [parameters: ('Zelphina', '900112233')]"
                )

        msg = ow.sanitize_db_error(FakeDBError())
        for value in ("Zelph", "Neil", PII_SSN, "parameters"):
            self.assertNotIn(value, msg)
        self.assertIn("member_first_name", msg)


# =============================================================================
# Parent/child integrity + transactions (fakes only)
# =============================================================================
class _Result:
    def fetchone(self):
        return None

    def fetchall(self):
        return []


class FakeConn:
    def __init__(self, engine: "FakeEngine") -> None:
        self.engine = engine

    def execute(self, sql, params=None):
        text_sql = str(sql)
        self.engine.pending.append((text_sql, params))
        if self.engine.fail_on and self.engine.fail_on in text_sql:
            raise RuntimeError("insert failed for value 'Zelphina' [parameters: ('900112233',)]")
        return _Result()


class FakeEngine:
    def __init__(self, fail_on: str | None = None) -> None:
        self.fail_on = fail_on
        self.pending: list = []
        self.committed: list = []
        self.commits = 0
        self.rollbacks = 0
        self.begins = 0

    @contextmanager
    def begin(self):
        self.begins += 1
        self.pending = []
        try:
            yield FakeConn(self)
        except Exception:
            self.rollbacks += 1
            self.pending = []
            raise
        self.commits += 1
        self.committed.extend(self.pending)
        self.pending = []


def _context(tmp: Path, run_mode: str = "load") -> OutboundRunContext:
    return OutboundRunContext(
        load_run_id="outbound_test_run", started_at=datetime.now(timezone.utc),
        run_mode=run_mode, source_mode="local", year_filter="2099", all_years=False,
        issuer_filter=None, month_filter="7", source_root=tmp / "outbound",
        git_commit=None, output_dir=tmp / "out",
    )


def _parsed_result(tmp: Path, fhash: str = "f" * 64):
    from outbound_automation.pipeline import FileProcessResult

    enriched = enrich(
        simple_doc(premiums=[premium_xml(2031, 1), premium_xml(2031, 2)]),
        attr=attribution(file_hash=fhash),
    )
    source = SourceFile(
        issuer=ISSUER, year="2099", month="07",
        file_name="to_12345_GA_834_INDV_20990701000000.xml",
        file_path=tmp / "x.xml", file_size=1, source_type="local",
    )
    return FileProcessResult(
        source=source, file_hash=fhash, parse_status="parsed",
        rows=enriched.rows, premium_rows=enriched.premium_rows,
        row_count=len(enriched.rows), premium_row_count=len(enriched.premium_rows),
    )


class IntegrityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.parent = {"file_hash": "h", "row_number_in_file": 1, "policy_id": "P",
                       "member_id": "M", "issuer": ISSUER, "load_run_id": "r"}
        self.child = {**self.parent, "premium_seq": 1}

    def test_valid(self) -> None:
        validate_parent_child("h", [self.parent], [self.child])

    def test_orphan_child(self) -> None:
        with self.assertRaises(ParentChildIntegrityError):
            validate_parent_child("h", [self.parent], [{**self.child, "row_number_in_file": 2}])

    def test_duplicate_seq(self) -> None:
        with self.assertRaises(ParentChildIntegrityError):
            validate_parent_child("h", [self.parent], [self.child, dict(self.child)])

    def test_child_identity_mismatch(self) -> None:
        with self.assertRaises(ParentChildIntegrityError):
            validate_parent_child("h", [self.parent], [{**self.child, "policy_id": "OTHER"}])

    def test_non_contiguous_parents(self) -> None:
        with self.assertRaises(ParentChildIntegrityError):
            validate_parent_child("h", [{**self.parent, "row_number_in_file": 2}], [])

    def test_wrong_file_hash(self) -> None:
        with self.assertRaises(ParentChildIntegrityError):
            validate_parent_child("other", [self.parent], [])


class TransactionTests(unittest.TestCase):
    def test_commit_writes_parents_children_and_file_log_together(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = FakeEngine()
            result = _parsed_result(Path(tmp))
            inserted, premium_inserted, _ = ow.load_file_rows(
                engine, _context(Path(tmp)), result, loaded_at=datetime.now(timezone.utc),
            )
            self.assertEqual((inserted, premium_inserted), (1, 2))
            self.assertEqual((engine.begins, engine.commits, engine.rollbacks), (1, 1, 0))
            sql = [s for s, _ in engine.committed]
            self.assertTrue(any("[outbound_automation] (" in s for s in sql))
            self.assertTrue(any("[outbound_automation_monthly_premium]" in s for s in sql))
            self.assertTrue(any("INSERT INTO [dbo].[outbound_automation_file_log]" in s for s in sql))
            self.assertEqual(result.parse_status, "loaded")

    def test_child_failure_rolls_back_whole_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = FakeEngine(fail_on="outbound_automation_monthly_premium")
            result = _parsed_result(Path(tmp))
            with self.assertRaises(RuntimeError):
                ow.load_file_rows(
                    engine, _context(Path(tmp)), result, loaded_at=datetime.now(timezone.utc),
                )
            self.assertEqual((engine.commits, engine.rollbacks), (0, 1))
            self.assertEqual(engine.committed, [])
            self.assertNotEqual(result.parse_status, "loaded")

    def test_orphan_child_rejected_before_transaction(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = FakeEngine()
            result = _parsed_result(Path(tmp))
            result.premium_rows[0]["row_number_in_file"] = 99
            with self.assertRaises(ParentChildIntegrityError):
                ow.load_file_rows(
                    engine, _context(Path(tmp)), result, loaded_at=datetime.now(timezone.utc),
                )
            self.assertEqual(engine.begins, 0)

    def test_upsert_file_log_cannot_mark_loaded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = _parsed_result(Path(tmp))
            result.parse_status = "loaded"
            with self.assertRaises(RuntimeError):
                ow.upsert_file_log(FakeEngine(), _context(Path(tmp)), result,
                                   loaded_at=datetime.now(timezone.utc))


# =============================================================================
# Pipeline (local synthetic files, fakes for Azure)
# =============================================================================
GOOD_NAME = "to_12345_GA_834_INDV_20990701000000.xml"


@contextmanager
def synthetic_root(files: dict[str, bytes]):
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        month = tmp_path / "outbound" / ISSUER / "2099" / "07"
        month.mkdir(parents=True)
        for name, data in files.items():
            (month / name).write_bytes(data)
        with patch("outbound_automation.discovery.resolve_outbound_extracted_root",
                   return_value=tmp_path / "extracted" / "outbound"), \
             patch.dict(os.environ, {"OUTBOUND_EXCHANGE_CODES": "GA"}, clear=False):
            yield tmp_path


def _status(result, name: str) -> str:
    return next(f.parse_status for f in result.file_results if f.source.file_name == name)


class PipelineDryRunTests(unittest.TestCase):
    def test_good_file_parsed(self) -> None:
        data = doc_xml([[enrollment_xml([
            enrollee_xml(member="M1", premiums=[premium_xml(2031, 1), premium_xml(2031, 2)]),
            enrollee_xml(member="M2", subscriber="N", premiums=[]),
        ])]])
        with synthetic_root({GOOD_NAME: data}) as tmp:
            result = run_dry_run(_context(tmp, "dry_run"), policy=FULL)
        self.assertEqual(_status(result, GOOD_NAME), "parsed")
        self.assertEqual(result.rows_parsed, 2)
        self.assertEqual(result.premium_rows_parsed, 2)
        self.assertEqual([r["row_number_in_file"] for r in result.rows], [1, 2])
        self.assertTrue(all("source_xml_json" not in r for r in result.rows))

    def test_zero_enrollee_file_fails(self) -> None:
        data = doc_xml([[]])
        with synthetic_root({GOOD_NAME: data}) as tmp:
            result = run_dry_run(_context(tmp, "dry_run"), policy=FULL)
        self.assertEqual(_status(result, GOOD_NAME), "failed")
        self.assertEqual(result.rows_parsed, 0)

    def test_malformed_xml_fails(self) -> None:
        with synthetic_root({GOOD_NAME: b"<enrollments><GS_Info>"}) as tmp:
            result = run_dry_run(_context(tmp, "dry_run"), policy=FULL)
        self.assertEqual(_status(result, GOOD_NAME), "failed")

    def test_issuer_mismatch_fails(self) -> None:
        data = doc_xml([[enrollment_xml([enrollee_xml()])]], header_issuer="54321")
        with synthetic_root({GOOD_NAME: data}) as tmp:
            result = run_dry_run(_context(tmp, "dry_run"), policy=FULL)
        self.assertEqual(_status(result, GOOD_NAME), "failed")
        self.assertIn("Issuer attribution conflict", result.file_results[0].error_message)

    def test_fatal_identity_fails_whole_file(self) -> None:
        data = doc_xml([[enrollment_xml([enrollee_xml(), enrollee_xml(member=None)])]])
        with synthetic_root({GOOD_NAME: data}) as tmp:
            result = run_dry_run(_context(tmp, "dry_run"), policy=FULL)
        self.assertEqual(_status(result, GOOD_NAME), "failed")
        self.assertEqual(result.file_results[0].rows, [])

    def test_duplicate_content_skipped_within_run(self) -> None:
        data = simple_doc()
        other = "to_12345_GA_834_INDV_20990702000000.xml"
        with synthetic_root({GOOD_NAME: data, other: data}) as tmp:
            result = run_dry_run(_context(tmp, "dry_run"), policy=FULL)
        statuses = sorted(f.parse_status for f in result.file_results)
        self.assertEqual(statuses, ["parsed", "skipped_duplicate"])

    def test_nonstandard_to_file_reported(self) -> None:
        with synthetic_root({GOOD_NAME: simple_doc(), "to_12345_odd.xml": simple_doc(member="X")}) as tmp:
            result = run_dry_run(_context(tmp, "dry_run"), policy=FULL)
        self.assertEqual([s.file_name for s, _ in result.rejected_files], ["to_12345_odd.xml"])
        self.assertEqual([c for _, c in result.rejected_files], ["to_nonstandard"])

    def test_no_member_dedup(self) -> None:
        data = doc_xml([[enrollment_xml([
            enrollee_xml(member="SAME", event_id="E1"),
            enrollee_xml(member="SAME", event_id="E1", event="001"),
        ])]])
        with synthetic_root({GOOD_NAME: data}) as tmp:
            result = run_dry_run(_context(tmp, "dry_run"), policy=FULL)
        self.assertEqual(result.rows_parsed, 2)

    def test_reports_are_pii_safe(self) -> None:
        data = doc_xml([[enrollment_xml([enrollee_xml(premiums=[premium_xml(2031, 1)])])]])
        with synthetic_root({GOOD_NAME: data}) as tmp:
            result = run_dry_run(_context(tmp, "dry_run"), policy=FULL)
        frames = [
            build_file_summary(result), build_warning_summary(result),
            build_transaction_summary(result), result.observations.paths_frame(),
            result.observations.codes_frame(),
        ]
        for frame in frames:
            text = frame.astype(str).to_string()
            for value in (PII_FIRST, PII_SSN, "MBR0001", "POL0001"):
                self.assertNotIn(value, text)
        for key in ("member_first_name", "member_last_name", "member_id", "policy_id",
                    "subscriber_id", "source_xml_json"):
            self.assertNotIn(key, REPORT_ROW_KEYS)


class PipelineLoadTests(unittest.TestCase):
    def _run(self, tmp: Path, engine: FakeEngine, loaded: dict[str, str]):
        with patch("outbound_automation.azure_common.require_env_gate"), \
             patch("outbound_automation.azure_common.connect_outbound_engine", return_value=engine), \
             patch("outbound_automation.azure_common.fast_executemany_enabled", return_value=False), \
             patch("outbound_automation.azure_writer.verify_tables_exist"), \
             patch("outbound_automation.azure_writer.verify_writer_columns"), \
             patch("outbound_automation.azure_writer.fetch_loaded_file_hashes", return_value=loaded), \
             patch("outbound_automation.azure_writer.insert_run_log_start"), \
             patch("outbound_automation.azure_writer.update_run_log_finish") as finish:
            result = run_load(_context(tmp), policy=FULL)
        return result, finish

    def test_db_failure_marks_file_failed_and_retryable(self) -> None:
        data = simple_doc(premiums=[premium_xml(2031, 1)])
        with synthetic_root({GOOD_NAME: data}) as tmp:
            engine = FakeEngine(fail_on="outbound_automation_monthly_premium")
            result, finish = self._run(tmp, engine, {})
        f = result.file_results[0]
        self.assertEqual(f.parse_status, "failed")
        self.assertIn("rolled back", f.error_message)
        self.assertNotIn(PII_FIRST, f.error_message)
        self.assertNotIn(PII_SSN, f.error_message)
        self.assertEqual(result.rows_inserted, 0)
        logged = [p for s, p in engine.committed if "outbound_automation_file_log" in s and p]
        self.assertEqual([p["parse_status"] for p in logged if "parse_status" in p], ["failed"])
        self.assertFalse(any("[outbound_automation] (" in s for s, _ in engine.committed))
        self.assertEqual(finish.call_args.kwargs["stats"]["files_failed"], 1)

    def test_successful_load_and_hash_idempotency(self) -> None:
        data = simple_doc(premiums=[premium_xml(2031, 1)])
        with synthetic_root({GOOD_NAME: data}) as tmp:
            engine = FakeEngine()
            result, _ = self._run(tmp, engine, {})
            self.assertEqual(result.file_results[0].parse_status, "loaded")
            self.assertEqual((result.rows_inserted, result.premium_rows_inserted), (1, 1))
            fhash = result.file_results[0].file_hash

            engine2 = FakeEngine()
            again, _ = self._run(tmp, engine2, {fhash: "outbound_prior"})
        self.assertEqual(again.file_results[0].parse_status, "skipped_duplicate")
        self.assertEqual(engine2.begins, 0)


# =============================================================================
# Isolation / static guards
# =============================================================================
def _production_sources() -> dict[Path, str]:
    files = sorted(OUTBOUND_PKG.glob("*.py")) + [OUTBOUND_PARSER]
    return {p: p.read_text(encoding="utf-8") for p in files}


class StaticGuardTests(unittest.TestCase):
    def test_no_inbound_parser_or_enrichment_dependency(self) -> None:
        for path, src in _production_sources().items():
            self.assertIsNone(re.search(r"\bParser834\b(?!Outbound)", src), path)
            self.assertNotIn("enrich_parser_row", src, path)
            self.assertNotIn("inbound_automation", src.replace("run_inbound_automation_load", ""), path)
            self.assertNotIn("parsers.parser_834 import", src, path)

    def test_no_sample_specific_constants(self) -> None:
        pattern = re.compile(r"\b(15105|2026|887|1102|1,102|9027|9,027)\b")
        for path, src in _production_sources().items():
            self.assertIsNone(pattern.search(src), f"{path}: {pattern.search(src)}")
            self.assertNotRegex(src, r"[\"']HLT[\"']", path)

    def test_no_foreign_key_or_alter_logic(self) -> None:
        for path, src in _production_sources().items():
            upper = src.upper()
            self.assertNotIn("ALTER TABLE", upper, path)
            self.assertNotIn("FOREIGN KEY", upper.replace("NO SQL FOREIGN KEY", ""), path)


class ConfigIsolationTests(unittest.TestCase):
    def _settings(self, source_root: Path) -> SimpleNamespace:
        return SimpleNamespace(
            source_data_path=source_root,
            extracted_path=source_root.parent / "extracted",
            refresh_from_env=lambda: None,
        )

    def test_remote_path_required_and_distinct(self) -> None:
        with patch("outbound_automation.config.settings", self._settings(Path("/tmp/sd"))), \
             patch.dict(os.environ, {"SFTP_OUTBOUND_REMOTE_PATH": ""}, clear=False):
            with self.assertRaises(OutboundConfigError):
                resolve_outbound_remote_path()
        env = {"SFTP_REMOTE_PATH": "/archive/in/good/834", "SFTP_OUTBOUND_REMOTE_PATH": "archive/in/good/834/"}
        with patch("outbound_automation.config.settings", self._settings(Path("/tmp/sd"))), \
             patch.dict(os.environ, env, clear=False):
            with self.assertRaises(OutboundConfigError):
                resolve_outbound_remote_path()

    def test_source_root_cannot_equal_inbound(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sd = Path(tmp) / "source_data"
            with patch("outbound_automation.config.settings", self._settings(sd)), \
                 patch.dict(os.environ, {"OUTBOUND_SOURCE_DATA_PATH": str(sd)}, clear=False):
                with self.assertRaises(OutboundConfigError):
                    resolve_outbound_source_root()

    def test_run_id_prefix(self) -> None:
        run_id = new_load_run_id(datetime(2031, 1, 2, 3, 4, 5, tzinfo=timezone.utc))
        self.assertRegex(run_id, r"^outbound_20310102_030405_[0-9a-f]{8}$")


if __name__ == "__main__":
    raise SystemExit(unittest.main())
