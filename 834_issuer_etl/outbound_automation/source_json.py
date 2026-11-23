"""
Lossless XML -> JSON serialization for source_xml_json / premium_json.

Each element becomes {"tag", "ns"?, "attrs"?, "text"?, "tail"?, "children"?}:
  * children keep document order and repetition;
  * attributes and namespace URIs are kept;
  * leaf text is the exact source text (not stripped);
  * whitespace-only formatting text between child elements is dropped.

Redaction is driven by SourceJsonPolicy (see outbound_automation.config). With the
"full" policy nothing is redacted.
"""

from __future__ import annotations

import hashlib
import json
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any

from parsers.parser_834_outbound import (
    ENROLLEE_TAG,
    ENROLLMENT_TAG,
    GS_TAG,
    OutboundEnrolleeRecord,
    element_children,
    local_name,
    namespace_uri,
)

SOURCE_JSON_FORMAT = "outbound_834_source_json_v1"
PREMIUM_JSON_FORMAT = "outbound_834_premium_json_v1"
REDACTED = "***REDACTED***"


@dataclass(frozen=True)
class SourceJsonPolicy:
    name: str
    redact_local_names: frozenset[str]


def element_to_json(
    el: ET.Element,
    *,
    redact: frozenset[str] = frozenset(),
    exclude_child_tags: frozenset[str] = frozenset(),
    _inherited_redact: bool = False,
) -> dict[str, Any]:
    name = local_name(el.tag)
    sensitive = _inherited_redact or name in redact
    node: dict[str, Any] = {"tag": name}
    ns = namespace_uri(el.tag)
    if ns:
        node["ns"] = ns
    if el.attrib:
        node["attrs"] = {k: (REDACTED if sensitive else v) for k, v in el.attrib.items()}

    kids = element_children(el)
    if el.text is not None and (not kids or el.text.strip()):
        node["text"] = REDACTED if sensitive and el.text.strip() else el.text

    children: list[dict[str, Any]] = []
    omitted: dict[str, int] = {}
    for child in kids:
        child_name = local_name(child.tag)
        if child_name in exclude_child_tags:
            omitted[child_name] = omitted.get(child_name, 0) + 1
            continue
        child_node = element_to_json(child, redact=redact, _inherited_redact=sensitive)
        if child.tail is not None and child.tail.strip():
            child_node["tail"] = REDACTED if sensitive else child.tail
        children.append(child_node)
    if children:
        node["children"] = children
    if omitted:
        node["omitted_children"] = omitted
    return node


def build_source_document(
    record: OutboundEnrolleeRecord,
    root: ET.Element,
    *,
    policy_name: str,
    redact: frozenset[str],
) -> dict[str, Any]:
    """Root header + containing GS_Info header + enrollment context + full enrollee subtree."""
    return {
        "format": SOURCE_JSON_FORMAT,
        "pii_policy": policy_name,
        "position": {
            "row_number_in_file": record.row_number_in_file,
            "gs_index": record.gs_index,
            "enrollment_index": record.enrollment_index,
            "enrollee_index": record.enrollee_index,
        },
        "root": element_to_json(root, redact=redact, exclude_child_tags=frozenset({GS_TAG})),
        "gs_info": element_to_json(
            record.gs_element, redact=redact, exclude_child_tags=frozenset({ENROLLMENT_TAG}),
        ),
        "enrollment": element_to_json(
            record.enrollment.element, redact=redact,
            exclude_child_tags=frozenset({ENROLLEE_TAG}),
        ),
        "enrollee": element_to_json(record.element, redact=redact),
    }


def dumps(doc: dict[str, Any]) -> str:
    return json.dumps(doc, ensure_ascii=False, separators=(",", ":"))


def serialize_enrollee(
    record: OutboundEnrolleeRecord,
    root: ET.Element,
    policy: SourceJsonPolicy,
) -> tuple[str, str]:
    """
    Return (source_xml_json, raw_record_hash).

    raw_record_hash is SHA-256 of the unredacted document, so it does not change
    when the redaction policy changes.
    """
    full = dumps(build_source_document(record, root, policy_name="full", redact=frozenset()))
    record_hash = hashlib.sha256(full.encode("utf-8")).hexdigest()
    if not policy.redact_local_names:
        if policy.name == "full":
            return full, record_hash
        doc = build_source_document(record, root, policy_name=policy.name, redact=frozenset())
        return dumps(doc), record_hash
    doc = build_source_document(
        record, root, policy_name=policy.name, redact=policy.redact_local_names,
    )
    return dumps(doc), record_hash


def serialize_premium(premium_el: ET.Element, policy: SourceJsonPolicy) -> str:
    return dumps(
        {
            "format": PREMIUM_JSON_FORMAT,
            "pii_policy": policy.name,
            "premium": element_to_json(premium_el, redact=policy.redact_local_names),
        }
    )
