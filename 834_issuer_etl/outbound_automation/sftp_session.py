"""
Outbound-only SFTP session: connect timeout, SSH keepalive, per-operation timeout,
SFTP_OP_START / SFTP_OP_END logging, a no-progress watchdog and bounded
reconnect/retry.

Stall detection is based on progress, never on total duration: every completed
operation and every received chunk resets the timer, so a long transfer that keeps
moving is never interrupted. After OUTBOUND_SFTP_STALL_WARN seconds without
progress a NO_PROGRESS warning is logged; after OUTBOUND_SFTP_STALL_ABORT seconds
the connection is closed, which unblocks the waiting call so it can be retried on a
fresh connection.

Used by outbound_automation.sftp_download only; the inbound SFTP code is untouched.
"""

from __future__ import annotations

import socket
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterator, TypeVar

import paramiko

from outbound_automation.config import SftpCredentials, SftpReliabilitySettings
from utils.logger import get_logger

logger = get_logger(__name__)

T = TypeVar("T")
Connector = Callable[[], "tuple[Any, Any]"]


class SftpStallError(RuntimeError):
    """The watchdog closed a connection that made no progress."""


class RetryableTransferError(RuntimeError):
    """A transfer completed but failed validation; retry on the same connection."""


class LocalWriteError(RuntimeError):
    """Writing/renaming a local file failed; retrying the download would not help."""


class SftpOperationFailed(RuntimeError):
    """A remote operation still failed after OUTBOUND_SFTP_MAX_RETRIES retries."""


class SftpReconnectLimitError(RuntimeError):
    """OUTBOUND_SFTP_MAX_RECONNECTS was reached; the run must stop."""


_NON_RETRYABLE = (
    SftpReconnectLimitError,
    SftpOperationFailed,
    LocalWriteError,
    FileNotFoundError,
    PermissionError,
    paramiko.AuthenticationException,
    paramiko.BadHostKeyException,
)
_RETRYABLE = (
    OSError,
    EOFError,
    paramiko.SSHException,
    SftpStallError,
    RetryableTransferError,
)


def is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, _NON_RETRYABLE):
        return False
    return isinstance(exc, _RETRYABLE)


def is_timeout(exc: BaseException) -> bool:
    return isinstance(exc, (socket.timeout, TimeoutError, SftpStallError))


def describe_error(exc: BaseException) -> str:
    text = str(exc).strip()
    return f"{type(exc).__name__}: {text}"[:300] if text else type(exc).__name__


@dataclass
class SessionStats:
    connects: int = 0
    reconnects: int = 0
    retries: int = 0
    stall_retries: int = 0
    stall_aborts: int = 0
    no_progress_warnings: int = 0


def _close_quietly(obj: Any) -> None:
    if obj is None:
        return
    try:
        obj.close()
    except Exception:  # noqa: BLE001 - best effort on an already broken connection
        pass


def paramiko_connector(creds: SftpCredentials, cfg: SftpReliabilitySettings) -> Connector:
    """Open transport + SFTP client with bounded connect/handshake/auth and op timeouts."""

    def connect() -> tuple[paramiko.Transport, paramiko.SFTPClient]:
        sock = socket.create_connection((creds.host, creds.port), timeout=cfg.connect_timeout)
        transport = paramiko.Transport(sock)
        try:
            transport.banner_timeout = cfg.connect_timeout
            transport.auth_timeout = cfg.connect_timeout
            transport.start_client(timeout=cfg.connect_timeout)
            if not transport.is_active():
                raise paramiko.SSHException("SSH negotiation did not complete")
            transport.auth_password(creds.username, creds.password)
            if cfg.keepalive:
                transport.set_keepalive(int(cfg.keepalive))
            client = paramiko.SFTPClient.from_transport(transport)
            if client is None:
                raise paramiko.SSHException("SFTP subsystem unavailable")
            client.get_channel().settimeout(cfg.op_timeout)
        except BaseException:
            transport.close()
            raise
        return transport, client

    return connect


class OutboundSftpSession:
    """One logical SFTP session that reconnects lazily after a failure."""

    def __init__(
        self,
        connector: Connector,
        settings: SftpReliabilitySettings,
        *,
        sleep: Callable[[float], Any] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        start_watchdog: bool = True,
    ) -> None:
        self.settings = settings
        self.stats = SessionStats()
        self.clock = clock
        self._connector = connector
        self._sleep = sleep
        self._start_watchdog = start_watchdog
        self._lock = threading.Lock()
        self._transport: Any = None
        self._client: Any = None
        self._activity: list[str] = []
        self._last_progress = clock()
        self._warned = False
        self._aborted = False
        self._stop = threading.Event()
        self._watchdog: threading.Thread | None = None

    def __enter__(self) -> "OutboundSftpSession":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ connection
    @property
    def client(self) -> Any:
        if self._client is None:
            self._connect()
        return self._client

    def _connect(self) -> None:
        if self.stats.connects > 0:
            if self.stats.reconnects >= self.settings.max_reconnects:
                raise SftpReconnectLimitError(
                    f"reconnect limit reached ({self.settings.max_reconnects}); stopping"
                )
            self.stats.reconnects += 1
            logger.warning(
                "SFTP_RECONNECT %d/%d", self.stats.reconnects, self.settings.max_reconnects,
            )
        logger.info("SFTP_CONNECT_START timeout=%gs", self.settings.connect_timeout)
        start = self.clock()
        with self.activity("connect"):
            transport, client = self._connector()
        with self._lock:
            self._transport, self._client = transport, client
            self._aborted = False
        self.stats.connects += 1
        logger.info("SFTP_CONNECT_END %.1fs", self.clock() - start)
        if self._start_watchdog and self._watchdog is None:
            self._watchdog = threading.Thread(
                target=self._watch, name="outbound-sftp-watchdog", daemon=True,
            )
            self._watchdog.start()

    def invalidate(self) -> None:
        """Drop the current connection; the next operation reconnects."""
        with self._lock:
            transport, self._transport, self._client = self._transport, None, None
        _close_quietly(transport)

    def close(self) -> None:
        self._stop.set()
        with self._lock:
            transport, client = self._transport, self._client
            self._transport = self._client = None
        _close_quietly(client)
        _close_quietly(transport)
        if self._watchdog is not None:
            self._watchdog.join(timeout=2.0)
            self._watchdog = None
        logger.info(
            "SFTP_CLOSE connects=%d reconnects=%d retries=%d stall_retries=%d",
            self.stats.connects, self.stats.reconnects, self.stats.retries,
            self.stats.stall_retries,
        )

    # ------------------------------------------------------------------ progress / watchdog
    @contextmanager
    def activity(self, label: str) -> Iterator[None]:
        with self._lock:
            self._activity.append(label)
            self._last_progress = self.clock()
            self._warned = False
        try:
            yield
        finally:
            with self._lock:
                self._activity.pop()
                self._last_progress = self.clock()

    def touch_progress(self) -> None:
        with self._lock:
            self._last_progress = self.clock()
            if self._warned:
                self._warned = False
                logger.info("SFTP_PROGRESS_RESUMED %s", self._activity[-1] if self._activity else "")

    def check_stall(self) -> str | None:
        """Return 'warn' / 'abort' when a threshold is crossed, else None."""
        now = self.clock()
        with self._lock:
            if not self._activity or self._aborted:
                return None
            idle = now - self._last_progress
            label = self._activity[-1]
            if idle >= self.settings.stall_abort:
                self._aborted = True
                transport = self._transport
                action = "abort"
            elif idle >= self.settings.stall_warn and not self._warned:
                self._warned = True
                action = "warn"
            else:
                return None
        if action == "warn":
            self.stats.no_progress_warnings += 1
            logger.warning(
                "WARN NO_PROGRESS %.0fs during %s (abort at %gs)",
                idle, label, self.settings.stall_abort,
            )
            return action
        self.stats.stall_aborts += 1
        logger.error(
            "SFTP_STALL_ABORT no progress for %.0fs during %s; closing connection", idle, label,
        )
        _close_quietly(transport)
        return action

    def _consume_abort(self) -> bool:
        with self._lock:
            aborted, self._aborted = self._aborted, False
        return aborted

    def _watch(self) -> None:
        interval = max(0.5, min(5.0, self.settings.stall_warn / 4))
        while not self._stop.wait(interval):
            try:
                self.check_stall()
            except Exception:  # noqa: BLE001 - the watchdog must never die
                logger.exception("SFTP watchdog error")

    # ------------------------------------------------------------------ operations
    def op(
        self,
        name: str,
        target: str,
        fn: Callable[[Any], T],
        *,
        note: Callable[[T], str] | None = None,
    ) -> T:
        """Run one remote call with START/END logging (no retry; see run())."""
        client = self.client
        logger.info("SFTP_OP_START %s %s", name, target)
        start = self.clock()
        with self.activity(f"{name} {target}"):
            try:
                value = fn(client)
            except Exception as exc:
                logger.warning(
                    "SFTP_OP_FAILED %s %s after %.1fs: %s",
                    name, target, self.clock() - start, describe_error(exc),
                )
                raise
        extra = f" {note(value)}" if note else ""
        logger.info("SFTP_OP_END %s %s %.1fs%s", name, target, self.clock() - start, extra)
        return value

    def run(self, label: str, fn: Callable[[], T]) -> T:
        """Call fn, retrying retryable failures with backoff (and reconnect when needed)."""
        attempt = 0
        while True:
            attempt += 1
            try:
                return fn()
            except Exception as exc:
                stalled = self._consume_abort()
                if not stalled and not is_retryable(exc):
                    raise
                reason = f"stall abort ({describe_error(exc)})" if stalled else describe_error(exc)
                reconnect = stalled or not isinstance(exc, RetryableTransferError)
                if reconnect:
                    self.invalidate()
                if attempt > self.settings.max_retries:
                    logger.error(
                        "SFTP_RETRY_EXHAUSTED %s after %d attempt(s): %s", label, attempt, reason,
                    )
                    raise SftpOperationFailed(
                        f"{label} failed after {attempt} attempt(s): {reason}"
                    ) from exc
                self.stats.retries += 1
                if stalled or is_timeout(exc):
                    self.stats.stall_retries += 1
                delay = self.settings.backoff_for(attempt)
                logger.warning(
                    "SFTP_RETRY %s attempt %d/%d failed: %s; retrying in %gs%s",
                    label, attempt, self.settings.max_retries + 1, reason, delay,
                    " on a new connection" if reconnect else "",
                )
                self._sleep(delay)

    def listdir(self, path: str) -> list[str]:
        return self.run(
            f"listdir {path}",
            lambda: self.op("listdir", path, lambda c: c.listdir(path), note=_entries),
        )

    def listdir_attr(self, path: str) -> list[Any]:
        return self.run(
            f"listdir_attr {path}",
            lambda: self.op("listdir_attr", path, lambda c: c.listdir_attr(path), note=_entries),
        )

    def stat(self, path: str) -> Any:
        return self.run(f"stat {path}", lambda: self.op("stat", path, lambda c: c.stat(path)))


def _entries(value: list[Any]) -> str:
    return f"entries={len(value)}"
