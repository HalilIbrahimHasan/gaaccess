"""Outbound-only configuration resolved from .env (read-only; inbound settings untouched)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from config.config import PROJECT_ROOT, settings
from outbound_automation.constants import (
    DEFAULT_SOURCE_SUBDIR,
    ENV_REMOTE_PATH,
    ENV_SOURCE_DATA_PATH,
)


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
