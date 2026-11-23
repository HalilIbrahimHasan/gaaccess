"""Per-file Azure insert timing metrics for outbound automation loads."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class FileInsertMetrics:
    row_count: int = 0
    premium_row_count: int = 0
    batch_size: int = 0
    batch_count: int = 0
    premium_batch_count: int = 0
    insert_sql_duration_ms: int = 0
    premium_insert_sql_duration_ms: int = 0
    file_log_duration_ms: int = 0
    commit_duration_ms: int = 0
    load_duration_ms: int = 0
    rows_per_sec: float = 0.0
    fast_executemany: bool = True
