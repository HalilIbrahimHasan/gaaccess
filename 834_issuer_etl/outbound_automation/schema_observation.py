"""
Dynamic schema observation for outbound TO XML.

Every element path seen in a run is compared with the reviewed registry
(known_paths_v1.json). Unknown paths, attributes and namespaces never fail
parsing; they become warnings and rows in schema_observations.xlsx. Only
structure is recorded: values are reduced to masked shapes (digits -> 9,
letters -> A) and suppressed entirely for sensitive tags. No SQL is altered.
"""

from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from outbound_automation.constants import KNOWN_PATHS_FILE
from parsers.parser_834_outbound import (
    ENROLLEE_TAG,
    ENROLLMENT_TAG,
    GS_TAG,
    ROOT_TAG,
    OutboundParseResult,
    element_children,
    local_name,
    namespace_uri,
)

W_UNKNOWN_XML_PATH = "UNKNOWN_XML_PATH"
W_UNKNOWN_XML_ATTRIBUTE = "UNKNOWN_XML_ATTRIBUTE"
W_XML_NAMESPACE_PRESENT = "XML_NAMESPACE_PRESENT"

SUPPRESSED = "<suppressed>"
EMPTY = "<empty>"
_MAX_SHAPE_LEN = 24
_MAX_SHAPES_PER_PATH = 3
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9]{1,10}$")

_GS_PATH = f"{ROOT_TAG}/{GS_TAG}"
_ENROLLMENT_PATH = f"{_GS_PATH}/{ENROLLMENT_TAG}"
_ENROLLEE_PATH = f"{_ENROLLMENT_PATH}/{ENROLLEE_TAG}"


@dataclass(frozen=True)
class KnownPathRegistry:
    version: int
    paths: frozenset[str]
    expected_namespaces: frozenset[str]
    expected_attributes: frozenset[str]

    @classmethod
    def load(cls, path: Path = KNOWN_PATHS_FILE) -> "KnownPathRegistry":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls(
            version=int(data.get("version", 0)),
            paths=frozenset(data.get("paths", [])),
            expected_namespaces=frozenset(data.get("expected_namespaces", [])),
            expected_attributes=frozenset(data.get("expected_attributes", [])),
        )

    def is_known(self, path: str) -> bool:
        return path in self.paths


def value_shape(text: str | None, local: str, suppress: frozenset[str]) -> str:
    if text is None or not text.strip():
        return EMPTY
    if local in suppress:
        return SUPPRESSED
    shape = re.sub(r"[0-9]", "9", text.strip())
    shape = re.sub(r"[^\W\d_]", "A", shape)
    if len(shape) > _MAX_SHAPE_LEN:
        shape = shape[:_MAX_SHAPE_LEN] + "..."
    return shape


def safe_code(code: str | None) -> str:
    """Codes are reported verbatim only when they look like short codes."""
    if code is None or not code.strip():
        return EMPTY
    value = code.strip()
    if _SAFE_CODE_RE.match(value):
        return value
    return value_shape(value, "", frozenset())


@dataclass
class _Stat:
    occurrences: int = 0
    files: set[str] = field(default_factory=set)
    first_file: str | None = None
    shapes: Counter = field(default_factory=Counter)

    def add(self, file_name: str, shape: str | None = None) -> None:
        self.occurrences += 1
        self.files.add(file_name)
        if self.first_file is None:
            self.first_file = file_name
        if shape is not None:
            self.shapes[shape] += 1


@dataclass
class FileStructure:
    unknown_paths: set[str] = field(default_factory=set)
    row_warnings: dict[int, list[str]] = field(default_factory=lambda: defaultdict(list))
    file_warnings: list[str] = field(default_factory=list)

    @property
    def unknown_path_count(self) -> int:
        return len(self.unknown_paths)


class SchemaObservationCollector:
    def __init__(
        self,
        registry: KnownPathRegistry,
        mapped: frozenset[str],
        suppress_local_names: frozenset[str],
    ) -> None:
        self.registry = registry
        self.mapped = mapped
        self.suppress = suppress_local_names
        self._paths: dict[str, _Stat] = defaultdict(_Stat)
        self._attributes: dict[tuple[str, str], _Stat] = defaultdict(_Stat)
        self._namespaces: dict[str, _Stat] = defaultdict(_Stat)
        self._codes: dict[tuple[str, str, bool], _Stat] = defaultdict(_Stat)

    # ------------------------------------------------------------------ observe
    def _walk(
        self,
        el: ET.Element,
        path: str,
        file_name: str,
        issues: set[str],
        unknown: set[str],
        *,
        exclude_child_tags: frozenset[str] = frozenset(),
    ) -> None:
        local = local_name(el.tag)
        kids = element_children(el)
        shape = None if kids else value_shape(el.text, local, self.suppress)
        self._paths[path].add(file_name, shape)
        if not self.registry.is_known(path):
            unknown.add(path)
            issues.add(W_UNKNOWN_XML_PATH)
        ns = namespace_uri(el.tag)
        if ns is not None:
            self._namespaces[ns].add(file_name)
            if ns not in self.registry.expected_namespaces:
                issues.add(W_XML_NAMESPACE_PRESENT)
        for attr in el.attrib:
            attr_name = local_name(attr)
            self._attributes[(path, attr_name)].add(file_name)
            if attr_name not in self.registry.expected_attributes:
                issues.add(W_UNKNOWN_XML_ATTRIBUTE)
        for child in kids:
            child_local = local_name(child.tag)
            if child_local in exclude_child_tags:
                continue
            self._walk(child, f"{path}/{child_local}", file_name, issues, unknown)

    def observe_file(self, file_name: str, parsed: OutboundParseResult) -> FileStructure:
        result = FileStructure()

        file_issues: set[str] = set()
        self._walk(
            parsed.root, ROOT_TAG, file_name, file_issues, result.unknown_paths,
            exclude_child_tags=frozenset({GS_TAG}),
        )
        for gs in parsed.gs_elements:
            self._walk(
                gs, _GS_PATH, file_name, file_issues, result.unknown_paths,
                exclude_child_tags=frozenset({ENROLLMENT_TAG}),
            )
        result.file_warnings = sorted(file_issues)

        for enrollment in parsed.enrollments:
            issues: set[str] = set()
            self._walk(
                enrollment.element, _ENROLLMENT_PATH, file_name, issues, result.unknown_paths,
                exclude_child_tags=frozenset({ENROLLEE_TAG}),
            )
            for row_number in enrollment.row_numbers:
                result.row_warnings[row_number].extend(sorted(issues))

        for record in parsed.enrollees:
            issues = set()
            self._walk(record.element, _ENROLLEE_PATH, file_name, issues, result.unknown_paths)
            for code in sorted(issues):
                if code not in result.row_warnings[record.row_number_in_file]:
                    result.row_warnings[record.row_number_in_file].append(code)
        return result

    def observe_code(self, category: str, code: str | None, file_name: str, *, known: bool) -> None:
        self._codes[(category, safe_code(code), known)].add(file_name)

    # ------------------------------------------------------------------ report
    @property
    def unknown_path_count(self) -> int:
        return sum(1 for p in self._paths if not self.registry.is_known(p))

    def paths_frame(self) -> pd.DataFrame:
        rows = []
        for path in sorted(set(self._paths) | set(self.registry.paths)):
            stat = self._paths.get(path)
            shapes = (
                "; ".join(f"{s} ({n})" for s, n in stat.shapes.most_common(_MAX_SHAPES_PER_PATH))
                if stat else ""
            )
            rows.append({
                "normalized_path": path,
                "known": self.registry.is_known(path),
                "mapped": path in self.mapped,
                "observed": stat is not None,
                "files_seen": len(stat.files) if stat else 0,
                "occurrence_count": stat.occurrences if stat else 0,
                "first_file": stat.first_file if stat else "",
                "value_shapes": shapes,
            })
        return pd.DataFrame(rows)

    def codes_frame(self) -> pd.DataFrame:
        rows = [
            {
                "category": category,
                "code": code,
                "known": known,
                "occurrence_count": stat.occurrences,
                "files_seen": len(stat.files),
                "first_file": stat.first_file,
            }
            for (category, code, known), stat in sorted(self._codes.items())
        ]
        return pd.DataFrame(rows)

    def attributes_namespaces_frame(self) -> pd.DataFrame:
        rows = [
            {
                "kind": "attribute",
                "normalized_path": path,
                "name": name,
                "expected": name in self.registry.expected_attributes,
                "occurrence_count": stat.occurrences,
                "files_seen": len(stat.files),
                "first_file": stat.first_file,
            }
            for (path, name), stat in sorted(self._attributes.items())
        ]
        rows += [
            {
                "kind": "namespace",
                "normalized_path": "",
                "name": uri,
                "expected": uri in self.registry.expected_namespaces,
                "occurrence_count": stat.occurrences,
                "files_seen": len(stat.files),
                "first_file": stat.first_file,
            }
            for uri, stat in sorted(self._namespaces.items())
        ]
        return pd.DataFrame(rows)
