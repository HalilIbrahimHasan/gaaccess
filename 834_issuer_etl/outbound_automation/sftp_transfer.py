"""
Outbound-only file transfer: completed-file skip, chunked download with progress,
size validation, decompression and atomic finalization.

Local layout is unchanged: <outbound source root>/{issuer}/{year}/{MM}/<name>.xml.
Temporary files are dot-prefixed, so local discovery never picks them up, and are
never treated as complete:
  .<remote filename>.part   bytes received from the server
  .<local name>.tmp         decompressed output (compressed remotes only)

An existing local file is only ever changed by an atomic os.replace() after the new
copy has been fully received, size-checked, decompressed (if needed) and flushed.

Skip rules (no remote read, and the local file is not opened, rewritten or hashed):
  plain .xml          local size == remote size
  .xml.gz / .xml.xz   the per-month state entry matches the remote name/size/mtime
                      and the local size; without an entry the file is fetched once.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import lzma
import os
import stat as stat_mod
import zlib
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO, Callable

from outbound_automation.config import SftpReliabilitySettings
from outbound_automation.constants import (
    DOWNLOAD_STATE_FILE,
    RUNNER_VERSION,
    SFTP_READ_PIECE_BYTES,
)
from outbound_automation.sftp_session import (
    LocalWriteError,
    OutboundSftpSession,
    RetryableTransferError,
    SftpOperationFailed,
    describe_error,
)
from utils.logger import get_logger

logger = get_logger(__name__)

ACTION_SKIP = "SKIP_ALREADY_DOWNLOADED"
ACTION_DOWNLOADED = "DOWNLOADED"
ACTION_REDOWNLOADED = "REDOWNLOADED"
ACTION_FAILED = "FAILED"

REASON_NEW = "NEW"
REASON_SIZE_MATCH = "LOCAL_SIZE_MATCHES_REMOTE"
REASON_STATE_MATCH = "STATE_MATCHES_REMOTE"
REASON_SIZE_MISMATCH = "LOCAL_SIZE_MISMATCH"
REASON_LEGACY_COMPRESSED = "LEGACY_COMPRESSED_NO_STATE"
REASON_STATE_MISMATCH = "STATE_MISMATCH"
REASON_FORCE = "FORCE_DOWNLOAD"
REASON_REMOTE_SIZE_UNKNOWN = "REMOTE_SIZE_UNKNOWN"
REASON_RECONNECT_LIMIT = "RECONNECT_LIMIT"

# The only keys ever written to the state file: file metadata, no XML content.
STATE_FIELDS = (
    "local_name",
    "remote_filename",
    "remote_path",
    "format",
    "remote_size",
    "remote_mtime",
    "local_size",
    "local_sha256",
    "completed_at_utc",
    "runner_version",
)

DOWNLOAD_LOG_COLUMNS = (
    "issuer", "year", "month", "index", "total", "filename", "local_name", "action",
    "reason", "remote_size", "local_size", "attempts", "seconds", "error",
)


@dataclass(frozen=True)
class RemoteFile:
    remote_path: str
    filename: str
    local_name: str
    format: str  # xml | gz | xz
    size: int | None
    mtime: int | None

    @property
    def compressed(self) -> bool:
        return self.format != "xml"

    @classmethod
    def from_entry(cls, entry: dict[str, Any]) -> "RemoteFile":
        return cls(
            remote_path=entry["remote_path"],
            filename=entry["filename"],
            local_name=entry["local_name"],
            format=entry["format"],
            size=_int_or_none(entry.get("remote_size")),
            mtime=_int_or_none(entry.get("remote_mtime")),
        )


@dataclass
class FileOutcome:
    index: int
    total: int
    filename: str
    local_name: str
    action: str
    reason: str
    remote_size: int | None
    local_size: int | None
    attempts: int = 0
    seconds: float = 0.0
    error: str = ""

    def as_row(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "total": self.total,
            "filename": self.filename,
            "local_name": self.local_name,
            "action": self.action,
            "reason": self.reason,
            "remote_size": self.remote_size,
            "local_size": self.local_size,
            "attempts": self.attempts,
            "seconds": round(self.seconds, 1),
            "error": self.error,
        }


@dataclass(frozen=True)
class Decision:
    skip: bool
    reason: str
    replaces_existing: bool
    local_size: int | None


@dataclass(frozen=True)
class _Completed:
    remote_size: int | None
    remote_mtime: int | None
    local_size: int
    local_sha256: str


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def part_path(local_month: Path, remote: RemoteFile) -> Path:
    return local_month / f".{remote.filename}.part"


def tmp_path(local_month: Path, remote: RemoteFile) -> Path:
    return local_month / f".{remote.local_name}.tmp"


# =============================================================================
# Download state (metadata only)
# =============================================================================
class DownloadStateStore:
    """Append-only JSON lines in <month>/.outbound_download_state.jsonl; last entry wins."""

    def __init__(self, local_month: Path) -> None:
        self.path = local_month / DOWNLOAD_STATE_FILE
        self._entries = self._load()

    def _load(self) -> dict[str, dict[str, Any]]:
        entries: dict[str, dict[str, Any]] = {}
        if not self.path.is_file():
            return entries
        unreadable = 0
        with self.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except ValueError:
                    unreadable += 1
                    continue
                if isinstance(row, dict) and row.get("local_name"):
                    entries[str(row["local_name"])] = row
                else:
                    unreadable += 1
        if unreadable:
            logger.warning("Ignored %d unreadable line(s) in %s", unreadable, self.path)
        return entries

    def get(self, local_name: str) -> dict[str, Any] | None:
        return self._entries.get(local_name)

    def record(self, **fields: Any) -> None:
        unknown = set(fields) - set(STATE_FIELDS)
        if unknown:
            raise ValueError(f"not allowed in download state: {sorted(unknown)}")
        row = {key: fields.get(key) for key in STATE_FIELDS}
        try:
            with self.path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, sort_keys=True) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
        except OSError as exc:
            raise LocalWriteError(f"cannot update {self.path.name}: {exc}") from exc
        self._entries[row["local_name"]] = row


def _state_matches(entry: dict[str, Any], remote: RemoteFile, local_size: int) -> bool:
    if entry.get("remote_filename") != remote.filename or remote.size is None:
        return False
    if _int_or_none(entry.get("remote_size")) != remote.size:
        return False
    if _int_or_none(entry.get("local_size")) != local_size:
        return False
    recorded_mtime = _int_or_none(entry.get("remote_mtime"))
    return remote.mtime is None or recorded_mtime is None or recorded_mtime == remote.mtime


def decide(
    remote: RemoteFile,
    local_path: Path,
    state_entry: dict[str, Any] | None,
    *,
    force_download: bool,
) -> Decision:
    """Skip or fetch, using only local metadata (the local file is never opened)."""
    try:
        st = local_path.stat()
        local_size = st.st_size if stat_mod.S_ISREG(st.st_mode) else None
    except FileNotFoundError:
        local_size = None
    exists = local_size is not None

    if force_download:
        return Decision(False, REASON_FORCE if exists else REASON_NEW, exists, local_size)
    if not exists:
        return Decision(False, REASON_NEW, False, None)
    if not remote.compressed:
        if remote.size is None:
            return Decision(False, REASON_REMOTE_SIZE_UNKNOWN, True, local_size)
        if local_size == remote.size:
            return Decision(True, REASON_SIZE_MATCH, False, local_size)
        return Decision(False, REASON_SIZE_MISMATCH, True, local_size)
    if state_entry is None:
        return Decision(False, REASON_LEGACY_COMPRESSED, True, local_size)
    if _state_matches(state_entry, remote, local_size):
        return Decision(True, REASON_STATE_MATCH, False, local_size)
    return Decision(False, REASON_STATE_MISMATCH, True, local_size)


# =============================================================================
# Progress
# =============================================================================
def _mb(value: float | None) -> str:
    return "unknown" if value is None else f"{value / (1024 * 1024):.1f} MB"


class TransferProgress:
    """[current/total] file remote=.. received=.. (pct) speed=.. elapsed=.. eta=.."""

    def __init__(
        self,
        index: int,
        total: int,
        filename: str,
        remote_size: int | None,
        *,
        interval: float,
        clock: Callable[[], float],
    ) -> None:
        self.index, self.total, self.filename = index, total, filename
        self.remote_size = remote_size
        self.interval = interval
        self._clock = clock
        self._start = clock()
        self._last_log = self._start

    def line(self, received: int, now: float | None = None) -> str:
        now = self._clock() if now is None else now
        elapsed = max(now - self._start, 0.0)
        speed = received / elapsed if elapsed > 0 else 0.0
        if self.remote_size:
            pct = f"{received * 100 / self.remote_size:.1f}%"
            eta = f"{max(self.remote_size - received, 0) / speed:.1f}s" if speed > 0 else "n/a"
        else:
            pct, eta = "n/a", "n/a"
        return (
            f"[{self.index}/{self.total}] {self.filename} remote={_mb(self.remote_size)} "
            f"received={_mb(received)} ({pct}) speed={_mb(speed)}/s "
            f"elapsed={elapsed:.1f}s eta={eta}"
        )

    def update(self, received: int) -> None:
        now = self._clock()
        if now - self._last_log >= self.interval:
            self._last_log = now
            logger.info(self.line(received, now))

    def finish(self, received: int) -> None:
        logger.info("%s DONE", self.line(received))


# =============================================================================
# Local file helpers (errors become LocalWriteError: never retried)
# =============================================================================
def _remove_quietly(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError as exc:
        logger.warning("Could not remove temporary file %s: %s", path.name, exc)


def _open_local(path: Path, buffering: int) -> BinaryIO:
    try:
        return open(path, "wb", buffering=buffering)
    except OSError as exc:
        raise LocalWriteError(f"cannot create {path.name}: {exc}") from exc


def _write_local(out: BinaryIO, data: bytes, path: Path) -> None:
    try:
        out.write(data)
    except OSError as exc:
        raise LocalWriteError(f"cannot write {path.name}: {exc}") from exc


def _flush_local(out: BinaryIO, path: Path) -> None:
    try:
        out.flush()
        os.fsync(out.fileno())
    except OSError as exc:
        raise LocalWriteError(f"cannot flush {path.name}: {exc}") from exc


def _close_local(out: BinaryIO) -> None:
    try:
        out.close()
    except OSError:
        pass


def _atomic_replace(src: Path, dst: Path) -> None:
    try:
        os.replace(src, dst)
    except OSError as exc:
        raise LocalWriteError(f"cannot move {src.name} -> {dst.name}: {exc}") from exc


def remove_stale_temp_files(local_month: Path, remote: RemoteFile) -> None:
    """Delete leftovers of an interrupted run; they are never used as complete files."""
    for path in (part_path(local_month, remote), tmp_path(local_month, remote)):
        try:
            size = path.stat().st_size
        except FileNotFoundError:
            continue
        except OSError:
            size = -1
        _remove_quietly(path)
        logger.warning("STALE_PART_REMOVED %s (%d bytes, incomplete; discarded)", path.name, size)


# =============================================================================
# Transfer
# =============================================================================
def _start_prefetch(handle: Any, size: int | None, chunk_bytes: int) -> None:
    prefetch = getattr(handle, "prefetch", None)
    if not size or prefetch is None:
        return
    window = max(1, chunk_bytes // SFTP_READ_PIECE_BYTES)
    try:
        prefetch(size, max_concurrent_requests=window)
    except TypeError:
        prefetch(size)


def _receive(
    session: OutboundSftpSession,
    remote: RemoteFile,
    part: Path,
    size: int | None,
    settings: SftpReliabilitySettings,
    progress: TransferProgress,
    digest: Any,
) -> int:
    """Stream the remote file into `part` in bounded pieces; return bytes received."""
    path = remote.remote_path
    received = 0
    with session.activity(f"read {remote.filename}"):
        handle = session.op("open", path, lambda c: c.open(path, "rb"))
        out = _open_local(part, settings.chunk_bytes)
        try:
            _start_prefetch(handle, size, settings.chunk_bytes)
            while True:
                piece = handle.read(SFTP_READ_PIECE_BYTES)
                if not piece:
                    break
                _write_local(out, piece, part)
                received += len(piece)
                if digest is not None:
                    digest.update(piece)
                session.touch_progress()
                progress.update(received)
            _flush_local(out, part)
        except LocalWriteError:
            try:
                handle.close()
            except Exception:  # noqa: BLE001 - best effort; the local error is what matters
                pass
            raise
        finally:
            _close_local(out)
        session.op("close", path, lambda c: handle.close())
    return received


def _decompress(part: Path, target: Path, remote: RemoteFile, chunk_bytes: int) -> tuple[int, str]:
    opener = gzip.open if remote.format == "gz" else lzma.open
    digest = hashlib.sha256()
    total = 0
    out = _open_local(target, chunk_bytes)
    try:
        try:
            src = opener(part, "rb")
        except (OSError, EOFError, lzma.LZMAError, zlib.error) as exc:
            raise RetryableTransferError(f"{remote.filename}: cannot decompress: {exc}") from exc
        with src:
            while True:
                try:
                    block = src.read(chunk_bytes)
                except (OSError, EOFError, lzma.LZMAError, zlib.error) as exc:
                    raise RetryableTransferError(
                        f"{remote.filename}: decompression failed: {describe_error(exc)}"
                    ) from exc
                if not block:
                    break
                _write_local(out, block, target)
                digest.update(block)
                total += len(block)
        _flush_local(out, target)
    finally:
        _close_local(out)
    return total, digest.hexdigest()


def _download_once(
    session: OutboundSftpSession,
    remote: RemoteFile,
    local_month: Path,
    local_path: Path,
    *,
    settings: SftpReliabilitySettings,
    keep_compressed: bool,
    index: int,
    total: int,
) -> _Completed:
    part = part_path(local_month, remote)
    _remove_quietly(part)
    st = session.op("stat", remote.remote_path, lambda c: c.stat(remote.remote_path))
    size = _int_or_none(getattr(st, "st_size", None))
    mtime = _int_or_none(getattr(st, "st_mtime", None))
    if size is not None and remote.size is not None and size != remote.size:
        logger.warning(
            "REMOTE_CHANGED_SINCE_LISTING %s listed=%d now=%d", remote.filename, remote.size, size,
        )
    size = remote.size if size is None else size
    mtime = remote.mtime if mtime is None else mtime

    progress = TransferProgress(
        index, total, remote.filename, size,
        interval=settings.progress_interval, clock=session.clock,
    )
    digest = None if remote.compressed else hashlib.sha256()
    try:
        received = _receive(session, remote, part, size, settings, progress, digest)
    except BaseException:
        _remove_quietly(part)
        raise
    progress.finish(received)
    if size is None:
        logger.warning("%s: remote size unknown; received %d bytes", remote.filename, received)
    elif received != size:
        _remove_quietly(part)
        raise RetryableTransferError(
            f"{remote.filename}: received {received} bytes, remote size is {size}"
        )

    if not remote.compressed:
        _atomic_replace(part, local_path)
        return _Completed(size, mtime, received, digest.hexdigest())

    target = tmp_path(local_month, remote)
    _remove_quietly(target)
    try:
        local_size, sha = _decompress(part, target, remote, settings.chunk_bytes)
        _atomic_replace(target, local_path)
    except BaseException:
        _remove_quietly(target)
        _remove_quietly(part)
        raise
    if keep_compressed:
        _atomic_replace(part, local_month / remote.filename)
        logger.info("Kept compressed copy: %s", local_month / remote.filename)
    else:
        _remove_quietly(part)
    return _Completed(size, mtime, local_size, sha)


def process_remote_file(
    session: OutboundSftpSession,
    remote: RemoteFile,
    local_month: Path,
    store: DownloadStateStore,
    *,
    settings: SftpReliabilitySettings,
    force_download: bool,
    keep_compressed: bool,
    index: int,
    total: int,
) -> FileOutcome:
    """Skip a verified complete file or download it; SftpReconnectLimitError propagates."""
    local_path = local_month / remote.local_name
    remove_stale_temp_files(local_month, remote)

    def outcome(action: str, reason: str, local_size: int | None, **extra: Any) -> FileOutcome:
        return FileOutcome(
            index, total, remote.filename, remote.local_name, action, reason,
            remote.size, local_size, **extra,
        )

    if remote.size is None:
        try:
            st = session.stat(remote.remote_path)
        except (SftpOperationFailed, FileNotFoundError, PermissionError) as exc:
            logger.error("[%d/%d] DOWNLOAD_FAILED %s: %s", index, total, remote.filename,
                         describe_error(exc))
            return outcome(ACTION_FAILED, REASON_REMOTE_SIZE_UNKNOWN, None, error=describe_error(exc))
        remote = replace(
            remote,
            size=_int_or_none(getattr(st, "st_size", None)),
            mtime=_int_or_none(getattr(st, "st_mtime", None)),
        )

    decision = decide(remote, local_path, store.get(remote.local_name), force_download=force_download)
    if decision.skip:
        logger.info(
            "[%d/%d] %s %s local=%d remote=%d (%s)",
            index, total, ACTION_SKIP, remote.filename, decision.local_size, remote.size,
            decision.reason,
        )
        return outcome(ACTION_SKIP, decision.reason, decision.local_size)

    action = ACTION_REDOWNLOADED if decision.replaces_existing else ACTION_DOWNLOADED
    logger.info(
        "[%d/%d] DOWNLOAD_START %s remote=%s reason=%s",
        index, total, remote.filename, _mb(remote.size), decision.reason,
    )
    attempts = 0
    start = session.clock()

    def attempt() -> _Completed:
        nonlocal attempts
        attempts += 1
        return _download_once(
            session, remote, local_month, local_path,
            settings=settings, keep_compressed=keep_compressed, index=index, total=total,
        )

    try:
        done = session.run(f"download {remote.filename}", attempt)
        store.record(
            local_name=remote.local_name,
            remote_filename=remote.filename,
            remote_path=remote.remote_path,
            format=remote.format,
            remote_size=done.remote_size,
            remote_mtime=done.remote_mtime,
            local_size=done.local_size,
            local_sha256=done.local_sha256,
            completed_at_utc=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            runner_version=RUNNER_VERSION,
        )
    except (SftpOperationFailed, LocalWriteError, FileNotFoundError, PermissionError) as exc:
        seconds = session.clock() - start
        logger.error(
            "[%d/%d] DOWNLOAD_FAILED %s after %d attempt(s), %.1fs: %s",
            index, total, remote.filename, attempts, seconds, describe_error(exc),
        )
        return outcome(
            ACTION_FAILED, decision.reason, decision.local_size,
            attempts=attempts, seconds=seconds, error=describe_error(exc),
        )

    seconds = session.clock() - start
    logger.info(
        "[%d/%d] %s %s local=%d bytes attempts=%d %.1fs",
        index, total, action, remote.filename, done.local_size, attempts, seconds,
    )
    return outcome(action, decision.reason, done.local_size, attempts=attempts, seconds=seconds)
