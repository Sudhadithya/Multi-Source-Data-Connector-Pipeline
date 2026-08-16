"""Timing, database, and environment helpers shared by the benchmarks.

Every measurement here is a median of repeated runs with explicit setup between
runs, so a single warm cache or an unlucky autovacuum cannot become the
headline number.
"""

import platform
import statistics
import sys
import time
from typing import Any, Callable, Dict, List, Optional

from sqlalchemy import text

from app.loader.postgres import engine
from benchmarks.generate import BENCH_CONNECTOR, BENCH_ID_PREFIX, BENCH_SOURCE

BENCH_TABLE = f"data_{BENCH_CONNECTOR}"


def timeit(fn: Callable[[], Any]) -> float:
    """Run `fn` once and return wall-clock seconds."""
    start = time.perf_counter()
    fn()
    return time.perf_counter() - start


def repeat(
    fn: Callable[[], Any],
    runs: int,
    setup: Optional[Callable[[], Any]] = None,
) -> Dict[str, Any]:
    """Run `fn` `runs` times, calling `setup` before each (untimed)."""
    durations: List[float] = []
    for _ in range(runs):
        if setup is not None:
            setup()
        durations.append(timeit(fn))
    return {
        "runs": runs,
        "median_s": statistics.median(durations),
        "min_s": min(durations),
        "max_s": max(durations),
        "all_s": durations,
    }


def throughput(records: int, seconds: float) -> float:
    return records / seconds if seconds > 0 else float("inf")


# --- database helpers -------------------------------------------------------

def exec_sql(statement: str) -> Any:
    with engine.begin() as conn:
        return conn.execute(text(statement))


def scalar(statement: str) -> Any:
    with engine.begin() as conn:
        return conn.execute(text(statement)).scalar()


def table_exists(name: str) -> bool:
    return bool(scalar(f"SELECT to_regclass('public.{name}') IS NOT NULL"))


def row_count(name: str) -> int:
    if not table_exists(name):
        return 0
    return int(scalar(f"SELECT count(*) FROM {name}"))


def reset_bench_table() -> None:
    """Empty the benchmark data table without dropping it."""
    if table_exists(BENCH_TABLE):
        exec_sql(f"TRUNCATE TABLE {BENCH_TABLE}")


def reset_bench_dlq() -> None:
    """Remove only this benchmark's rows from the shared dead-letter table."""
    if table_exists("failed_records"):
        exec_sql(
            "DELETE FROM failed_records "
            f"WHERE source = '{BENCH_SOURCE}' AND id LIKE '{BENCH_ID_PREFIX}%'"
        )


def bench_dlq_count() -> int:
    if not table_exists("failed_records"):
        return 0
    return int(scalar(
        "SELECT count(*) FROM failed_records "
        f"WHERE source = '{BENCH_SOURCE}' AND id LIKE '{BENCH_ID_PREFIX}%'"
    ))


def table_digest(name: str, normalize_json: bool = False) -> Optional[str]:
    """md5 over every row, order-independent.

    Two load strategies that produce the same digest produced the same data.
    `normalize_json=True` compares `raw_data` as jsonb, which ignores key order
    and whitespace — so a mismatch on the strict digest but a match on the
    normalized one means the serialization differs, not the content.
    """
    if not table_exists(name):
        return None
    raw_expr = "raw_data::jsonb::text" if normalize_json else "raw_data::text"
    return scalar(rf"""
        SELECT md5(coalesce(string_agg(row_repr, E'\n' ORDER BY row_repr), ''))
        FROM (
            SELECT id || '|' || source
                   || '|' || coalesce(title, '<NULL>')
                   || '|' || coalesce(description, '<NULL>')
                   || '|' || created_at::text
                   || '|' || {raw_expr} AS row_repr
            FROM {name}
        ) s
    """)


def table_size_pretty(name: str) -> Optional[str]:
    if not table_exists(name):
        return None
    return scalar(f"SELECT pg_size_pretty(pg_total_relation_size('{name}'))")


# --- environment ------------------------------------------------------------

def env_info() -> Dict[str, Any]:
    pg_version = scalar("SELECT version()")
    settings = {}
    with engine.begin() as conn:
        for name in ("shared_buffers", "synchronous_commit", "fsync", "max_wal_size"):
            settings[name] = conn.execute(text(f"SHOW {name}")).scalar()
    return {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "processor": platform.processor(),
        "cpu_count": __import__("os").cpu_count(),
        "postgres": pg_version.split(" on ")[0] if pg_version else None,
        "postgres_settings": settings,
    }
