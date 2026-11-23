"""Outbound-only configuration resolved from .env (read-only; inbound settings untouched)."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from config.config import PROJECT_ROOT, settings
from outbound_automation.constants import (
    DEFAULT_EXCHANGE_CODES,
    DEFAULT_SOURCE_SUBDIR,
    ENV_EXCHANGE_CODES,
    ENV_REMOTE_PATH,
    ENV_SOURCE_DATA_PATH,
    ENV_SOURCE_JSON_PII_POLICY,
    PII_POLICIES,
    PII_POLICY_FULL,
    PII_POLICY_UNSET_DRY_RUN,
    SENSITIVE_TAGS_FILE,
)
from outbound_automation.source_json import SourceJsonPolicy


class OutboundConfigError(RuntimeError):
    """Outbound configuration is missing or would overlap the inbound pipeline."""


def _clean(raw: str | None) -> str:
    return (raw or "").strip().strip('"').strip("'")


def _normalize_remote(path: str) -> str:
    return path.strip().strip("/")


def resolve_outbound_remote_path() -> str:
    """
    Return SFTP_OUTBOUND_REMOTE_PATH.

    Never falls back to the inbound SFTP_REMOTE_PATH, and refuses to run when both
    variables point at the same remote root.
    """
    settings.refresh_from_env()
    remote = _clean(os.getenv(ENV_REMOTE_PATH))
    if not remote:
        raise OutboundConfigError(
            f"{ENV_REMOTE_PATH} is not set. Add e.g. "
            f"{ENV_REMOTE_PATH}=archive/out/good/834 to .env (inbound SFTP_REMOTE_PATH is never used)."
        )
    inbound_remote = _clean(os.getenv("SFTP_REMOTE_PATH"))
    if inbound_remote and _normalize_remote(inbound_remote) == _normalize_remote(remote):
        raise OutboundConfigError(
            f"{ENV_REMOTE_PATH} ({remote}) equals inbound SFTP_REMOTE_PATH ({inbound_remote}). "
            "Outbound must read a different remote root."
        )
    return remote


def resolve_outbound_source_root() -> Path:
    """
    Local root for downloaded outbound TO files.

    Default: <SOURCE_DATA_PATH>/outbound, laid out as {issuer}/{year}/{MM}/*.xml
    exactly like the inbound source_data partitions.
    """
    settings.refresh_from_env()
    inbound_root = settings.source_data_path.resolve()
    raw = _clean(os.getenv(ENV_SOURCE_DATA_PATH))
    if raw:
        p = Path(raw)
        root = p if p.is_absolute() else PROJECT_ROOT / p
    else:
        root = settings.source_data_path / DEFAULT_SOURCE_SUBDIR
    root = root.resolve()

    if root == inbound_root:
        raise OutboundConfigError(
            f"Outbound source root ({root}) equals inbound SOURCE_DATA_PATH. "
            "Inbound and outbound downloads must not share a folder."
        )
    if root.parent == inbound_root and root.name.isdigit() and len(root.name) == 5:
        raise OutboundConfigError(
            f"Outbound source root ({root}) looks like an inbound issuer folder."
        )
    return root


def resolve_outbound_extracted_root() -> Path:
    """Zip expansion target for outbound discovery (kept apart from inbound extracted/)."""
    return settings.extracted_path / DEFAULT_SOURCE_SUBDIR


@dataclass(frozen=True)
class SftpCredentials:
    host: str
    port: int
    username: str
    password: str


def resolve_sftp_credentials() -> SftpCredentials:
    """Shared SFTP server credentials (SFTP_HOST/PORT/USERNAME/PASSWORD), read-only."""
    settings.refresh_from_env()
    host = _clean(os.getenv("SFTP_HOST"))
    user = _clean(os.getenv("SFTP_USERNAME") or os.getenv("SFTP_USER"))
    password = os.getenv("SFTP_PASSWORD", "")
    port_raw = _clean(os.getenv("SFTP_PORT")) or "22"
    if not host or not user:
        raise OutboundConfigError(
            "SFTP credentials missing. SFTP_HOST and SFTP_USERNAME are required."
        )
    return SftpCredentials(host=host, port=int(port_raw), username=user, password=password)


def parse_exchange_codes(raw: str | None) -> frozenset[str]:
    codes = {c.strip().upper() for c in (raw or "").split(",") if c.strip()}
    return frozenset(codes) if codes else frozenset(DEFAULT_EXCHANGE_CODES)


def resolve_exchange_codes() -> frozenset[str]:
    """Exchange code(s) accepted in TO filenames (OUTBOUND_EXCHANGE_CODES, comma-separated)."""
    return parse_exchange_codes(os.getenv(ENV_EXCHANGE_CODES))


def load_sensitive_local_names(path: Path = SENSITIVE_TAGS_FILE) -> frozenset[str]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return frozenset(str(n) for n in data.get("sensitive_local_names", []))


def resolve_source_json_policy(*, required: bool) -> SourceJsonPolicy:
    """
    Policy for source_xml_json / premium_json (OUTBOUND_SOURCE_JSON_PII_POLICY).

      full    -> lossless, nothing redacted
      redact  -> values of sensitive_tags_v1.json local names replaced by a marker

    No default policy exists. required=True (--load) refuses to run while unset.
    required=False (--dry-run) builds JSON in memory only (never persisted or
    reported) with the redaction list applied.
    """
    settings.refresh_from_env()
    raw = _clean(os.getenv(ENV_SOURCE_JSON_PII_POLICY)).lower()
    if not raw:
        if required:
            raise OutboundConfigError(
                f"{ENV_SOURCE_JSON_PII_POLICY} is not set. source_xml_json contains member PII; "
                f"an approved policy ({' | '.join(PII_POLICIES)}) must be configured before --load."
            )
        return SourceJsonPolicy(
            name=PII_POLICY_UNSET_DRY_RUN, redact_local_names=load_sensitive_local_names(),
        )
    if raw not in PII_POLICIES:
        raise OutboundConfigError(
            f"{ENV_SOURCE_JSON_PII_POLICY}={raw!r} is invalid; expected one of {', '.join(PII_POLICIES)}."
        )
    if raw == PII_POLICY_FULL:
        return SourceJsonPolicy(name=raw, redact_local_names=frozenset())
    return SourceJsonPolicy(name=raw, redact_local_names=load_sensitive_local_names())
