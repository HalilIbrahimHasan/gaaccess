"""Outbound-only configuration resolved from .env (read-only; inbound settings untouched)."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from config.config import PROJECT_ROOT, settings
from outbound_automation.constants import (
    DEFAULT_EXCHANGE_CODES,
    DEFAULT_SFTP_CHUNK_BYTES,
    DEFAULT_SFTP_CONNECT_TIMEOUT,
    DEFAULT_SFTP_KEEPALIVE,
    DEFAULT_SFTP_MAX_RECONNECTS,
    DEFAULT_SFTP_MAX_RETRIES,
    DEFAULT_SFTP_OP_TIMEOUT,
    DEFAULT_SFTP_PROGRESS_INTERVAL,
    DEFAULT_SFTP_STALL_ABORT,
    DEFAULT_SFTP_STALL_WARN,
    DEFAULT_SOURCE_SUBDIR,
    ENV_EXCHANGE_CODES,
    ENV_REMOTE_PATH,
    ENV_SFTP_CHUNK_BYTES,
    ENV_SFTP_CONNECT_TIMEOUT,
    ENV_SFTP_KEEPALIVE,
    ENV_SFTP_MAX_RECONNECTS,
    ENV_SFTP_MAX_RETRIES,
    ENV_SFTP_OP_TIMEOUT,
    ENV_SFTP_PROGRESS_INTERVAL,
    ENV_SFTP_STALL_ABORT,
    ENV_SFTP_STALL_WARN,
    ENV_SOURCE_DATA_PATH,
    ENV_SOURCE_JSON_PII_POLICY,
    PII_POLICIES,
    PII_POLICY_FULL,
    PII_POLICY_UNSET_DRY_RUN,
    SENSITIVE_TAGS_FILE,
    SFTP_READ_PIECE_BYTES,
    SFTP_RETRY_BACKOFF_SECONDS,
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


@dataclass(frozen=True)
class SftpReliabilitySettings:
    """Timeouts and retry limits for the outbound SFTP session (seconds unless noted)."""

    connect_timeout: float = DEFAULT_SFTP_CONNECT_TIMEOUT
    keepalive: int = DEFAULT_SFTP_KEEPALIVE
    op_timeout: float = DEFAULT_SFTP_OP_TIMEOUT
    stall_warn: float = DEFAULT_SFTP_STALL_WARN
    stall_abort: float = DEFAULT_SFTP_STALL_ABORT
    max_retries: int = DEFAULT_SFTP_MAX_RETRIES
    max_reconnects: int = DEFAULT_SFTP_MAX_RECONNECTS
    chunk_bytes: int = DEFAULT_SFTP_CHUNK_BYTES
    progress_interval: float = DEFAULT_SFTP_PROGRESS_INTERVAL
    retry_backoff: tuple[float, ...] = SFTP_RETRY_BACKOFF_SECONDS

    def backoff_for(self, retry_number: int) -> float:
        """Wait before retry `retry_number` (1-based)."""
        if not self.retry_backoff:
            return 0.0
        return self.retry_backoff[min(retry_number, len(self.retry_backoff)) - 1]

    def describe(self) -> list[tuple[str, str]]:
        waits = ", ".join(f"{self.backoff_for(n):g}s" for n in range(1, self.max_retries + 1))
        return [
            ("connect timeout", f"{self.connect_timeout:g}s"),
            ("SSH keepalive", f"{self.keepalive}s" if self.keepalive else "off"),
            ("operation timeout", f"{self.op_timeout:g}s"),
            ("no-progress warning", f"{self.stall_warn:g}s"),
            ("stall abort", f"{self.stall_abort:g}s"),
            ("retries per op/file", f"{self.max_retries} (waits: {waits or 'none'})"),
            ("max reconnects", str(self.max_reconnects)),
            ("chunk / read-ahead", f"{self.chunk_bytes} bytes"),
            ("progress interval", f"{self.progress_interval:g}s"),
        ]


def _env_number(name: str, default, cast, minimum):
    raw = _clean(os.getenv(name))
    if not raw:
        return default
    try:
        value = cast(raw)
    except ValueError:
        raise OutboundConfigError(f"{name}={raw!r} is not a valid number.") from None
    if value < minimum:
        raise OutboundConfigError(f"{name}={raw!r} must be >= {minimum}.")
    return value


def resolve_sftp_reliability_settings() -> SftpReliabilitySettings:
    """OUTBOUND_SFTP_* timeouts/retry limits; every value has a safe default."""
    settings.refresh_from_env()
    resolved = SftpReliabilitySettings(
        connect_timeout=_env_number(ENV_SFTP_CONNECT_TIMEOUT, DEFAULT_SFTP_CONNECT_TIMEOUT, float, 1.0),
        keepalive=_env_number(ENV_SFTP_KEEPALIVE, DEFAULT_SFTP_KEEPALIVE, int, 0),
        op_timeout=_env_number(ENV_SFTP_OP_TIMEOUT, DEFAULT_SFTP_OP_TIMEOUT, float, 1.0),
        stall_warn=_env_number(ENV_SFTP_STALL_WARN, DEFAULT_SFTP_STALL_WARN, float, 1.0),
        stall_abort=_env_number(ENV_SFTP_STALL_ABORT, DEFAULT_SFTP_STALL_ABORT, float, 1.0),
        max_retries=_env_number(ENV_SFTP_MAX_RETRIES, DEFAULT_SFTP_MAX_RETRIES, int, 0),
        max_reconnects=_env_number(ENV_SFTP_MAX_RECONNECTS, DEFAULT_SFTP_MAX_RECONNECTS, int, 0),
        chunk_bytes=_env_number(
            ENV_SFTP_CHUNK_BYTES, DEFAULT_SFTP_CHUNK_BYTES, int, SFTP_READ_PIECE_BYTES,
        ),
        progress_interval=_env_number(
            ENV_SFTP_PROGRESS_INTERVAL, DEFAULT_SFTP_PROGRESS_INTERVAL, float, 1.0,
        ),
    )
    if resolved.stall_warn >= resolved.stall_abort:
        raise OutboundConfigError(
            f"{ENV_SFTP_STALL_WARN} ({resolved.stall_warn:g}) must be lower than "
            f"{ENV_SFTP_STALL_ABORT} ({resolved.stall_abort:g})."
        )
    return resolved


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
