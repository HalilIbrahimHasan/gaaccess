#!/usr/bin/env python3
"""Offline tests for outbound automation isolation (no SFTP, no Azure)."""

from __future__ import annotations

import os
import re
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from connectors.base_connector import SourceFile  # noqa: E402
from inbound_automation.azure_writer import INBOUND_AUTOMATION_COLUMNS  # noqa: E402
from ingestion.file_discovery import discover_source_files  # noqa: E402
from outbound_automation import azure_writer as ow  # noqa: E402
from outbound_automation.config import (  # noqa: E402
    OutboundConfigError,
    resolve_outbound_remote_path,
    resolve_outbound_source_root,
)
from outbound_automation.constants import OUTBOUND_TABLES  # noqa: E402
from outbound_automation.discovery import discover_outbound_files  # noqa: E402
from outbound_automation.file_rules import (  # noqa: E402
    classify_outbound_filename,
    is_outbound_to_file,
)
from outbound_automation.pipeline import _process_file  # noqa: E402
from outbound_automation.run_context import OutboundRunContext, new_load_run_id  # noqa: E402

OUTBOUND_DDL = ROOT / "sql" / "outbound_automation_ddl.sql"


class ToFileRuleTests(unittest.TestCase):
    def test_to_files_selected(self) -> None:
        self.assertEqual(classify_outbound_filename("to_15105_x.xml"), "valid_xml")
        self.assertEqual(classify_outbound_filename("to_15105_x.xml.gz"), "valid_gz")
        self.assertEqual(classify_outbound_filename("to_15105_x.xml.xz"), "valid_xz")
        self.assertTrue(is_outbound_to_file("TO_15105_x.XML"))

    def test_from_files_rejected(self) -> None:
        name = "from_68806_GA_834_INDV_2026-02-08T05344500.P.xml"
        self.assertEqual(classify_outbound_filename(name), "from")
        self.assertFalse(is_outbound_to_file(name))

    def test_non_834_to_files_rejected(self) -> None:
        for name in ("to_15105.edi", "to_15105_report.xml", "to_15105_tracking.xml",
                     "to_15105_summary.xml", "to_15105.txt", "to_15105.dtl"):
            self.assertFalse(is_outbound_to_file(name), name)

    def test_unprefixed_rejected(self) -> None:
        self.assertEqual(classify_outbound_filename("15105_GA_834.xml"), "other")


class RunIdTests(unittest.TestCase):
    def test_outbound_prefix(self) -> None:
        run_id = new_load_run_id(datetime(2026, 11, 23, 5, 0, 0, tzinfo=timezone.utc))
        self.assertRegex(run_id, r"^outbound_20261123_050000_[0-9a-f]{8}$")


class WriterIsolationTests(unittest.TestCase):
    def _all_sql(self) -> list[str]:
        return [
            str(ow._RUN_LOG_INSERT_SQL),
            str(ow._RUN_LOG_UPDATE_SQL),
            str(ow._FILE_LOG_INSERT_SQL),
            str(ow._FILE_LOG_UPDATE_SQL),
            str(ow._FILE_LOG_EXISTS_SQL),
            str(ow._row_insert_sql()),
        ]

    def test_sql_targets_only_outbound(self) -> None:
        for sql in self._all_sql():
            self.assertNotIn("inbound", sql.lower())
            targets = set(re.findall(r"\[dbo\]\.\[(\w+)\]", sql))
            self.assertTrue(targets)
            self.assertTrue(targets <= set(OUTBOUND_TABLES), targets)

    def test_allowlist_rejects_inbound(self) -> None:
        for table in ("inbound_automation", "inbound_automation_run_log",
                      "inbound_automation_file_log"):
            with self.assertRaises(RuntimeError):
                ow._assert_outbound_target(table)

    def test_columns_mirror_inbound_writer(self) -> None:
        self.assertEqual(ow.OUTBOUND_AUTOMATION_COLUMNS, INBOUND_AUTOMATION_COLUMNS)

    def test_columns_match_outbound_ddl(self) -> None:
        ddl = OUTBOUND_DDL.read_text(encoding="utf-8")
        block = ddl.split("CREATE TABLE dbo.outbound_automation (", 1)[1].split("CONSTRAINT PK_", 1)[0]
        ddl_cols = {
            m.group(1)
            for m in re.finditer(r"^\s{8}(\w+)\s+(?:BIGINT|NVARCHAR|INT|DATETIME2|DATE|DECIMAL)",
                                 block, re.MULTILINE)
        }
        ddl_cols.discard("id")
        self.assertEqual(ddl_cols, set(ow.OUTBOUND_AUTOMATION_COLUMNS))


class ConfigIsolationTests(unittest.TestCase):
    def _settings(self, source_root: Path) -> SimpleNamespace:
        return SimpleNamespace(
            source_data_path=source_root,
            extracted_path=source_root.parent / "extracted",
            refresh_from_env=lambda: None,
        )

    def test_remote_path_required(self) -> None:
        with patch("outbound_automation.config.settings", self._settings(Path("/tmp/sd"))), \
             patch.dict(os.environ, {"SFTP_OUTBOUND_REMOTE_PATH": ""}, clear=False):
            with self.assertRaises(OutboundConfigError):
                resolve_outbound_remote_path()

    def test_remote_path_must_differ_from_inbound(self) -> None:
        env = {
            "SFTP_REMOTE_PATH": "/archive/in/good/834",
            "SFTP_OUTBOUND_REMOTE_PATH": "archive/in/good/834/",
        }
        with patch("outbound_automation.config.settings", self._settings(Path("/tmp/sd"))), \
             patch.dict(os.environ, env, clear=False):
            with self.assertRaises(OutboundConfigError):
                resolve_outbound_remote_path()

    def test_remote_path_outbound(self) -> None:
        env = {
            "SFTP_REMOTE_PATH": "archive/in/good/834",
            "SFTP_OUTBOUND_REMOTE_PATH": "archive/out/good/834",
        }
        with patch("outbound_automation.config.settings", self._settings(Path("/tmp/sd"))), \
             patch.dict(os.environ, env, clear=False):
            self.assertEqual(resolve_outbound_remote_path(), "archive/out/good/834")

    def test_default_source_root_is_outbound_subfolder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sd = Path(tmp) / "source_data"
            with patch("outbound_automation.config.settings", self._settings(sd)), \
                 patch.dict(os.environ, {"OUTBOUND_SOURCE_DATA_PATH": ""}, clear=False):
                self.assertEqual(resolve_outbound_source_root(), (sd / "outbound").resolve())

    def test_source_root_cannot_equal_inbound(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sd = Path(tmp) / "source_data"
            with patch("outbound_automation.config.settings", self._settings(sd)), \
                 patch.dict(os.environ, {"OUTBOUND_SOURCE_DATA_PATH": str(sd)}, clear=False):
                with self.assertRaises(OutboundConfigError):
                    resolve_outbound_source_root()


class LocalIsolationTests(unittest.TestCase):
    def test_inbound_and_outbound_discovery_do_not_mix(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sd = Path(tmp) / "source_data"
            inbound_month = sd / "15105" / "2026" / "02"
            outbound_month = sd / "outbound" / "15105" / "2026" / "02"
            inbound_month.mkdir(parents=True)
            outbound_month.mkdir(parents=True)
            (inbound_month / "from_15105_GA_834_INDV_20260201000000.xml").write_bytes(b"<r/>")
            (outbound_month / "to_15105_sample.xml").write_bytes(b"<r/>")
            (outbound_month / "from_15105_stray.xml").write_bytes(b"<r/>")

            inbound = discover_source_files(sd, year_filter="2026", month_filter="02")
            self.assertEqual([f.file_name for f in inbound],
                             ["from_15105_GA_834_INDV_20260201000000.xml"])

            with patch("outbound_automation.discovery.resolve_outbound_extracted_root",
                       return_value=Path(tmp) / "extracted" / "outbound"):
                outbound = discover_outbound_files(
                    sd / "outbound", year_filter="2026", all_years=False,
                    issuer_filter=None, month_filter="2",
                )
            self.assertEqual([f.file_name for f in outbound], ["to_15105_sample.xml"])


class ZeroRowTests(unittest.TestCase):
    def test_zero_row_to_file_is_failed_not_loaded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "to_15105_empty.xml"
            path.write_bytes(b"<root><other/></root>")
            source = SourceFile(
                issuer="15105", year="2026", month="02", file_name=path.name,
                file_path=path, file_size=path.stat().st_size, source_type="local",
            )
            ctx = OutboundRunContext(
                load_run_id="outbound_test", started_at=datetime.now(timezone.utc),
                run_mode="dry_run", source_mode="local", year_filter="2026",
                all_years=False, issuer_filter=None, month_filter="02",
                source_root=Path(tmp), git_commit=None, output_dir=Path(tmp),
            )
            from parsers.parser_834 import Parser834

            result = _process_file(
                source, parser=Parser834(), context=ctx, cli_year="2026",
                loaded_at=datetime.now(timezone.utc),
            )
            self.assertEqual(result.parse_status, "failed")
            self.assertEqual(result.row_count, 0)


if __name__ == "__main__":
    raise SystemExit(unittest.main())
