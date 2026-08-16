"""Synthetic record generation for the transform/load benchmarks.

Records are shaped like the GitHub connector's raw output on purpose, so they
flow through the real `Transformer.transform` branch instead of a
benchmark-only code path. Every generated id is prefixed with `bench-` so
benchmark rows can be cleaned out of the shared `failed_records` table without
touching real data.
"""

import json
import random
import string
from typing import Any, Dict, List

# Records are transformed as "github" so the real transformer branch runs.
BENCH_SOURCE = "github"

# Rows are loaded into `data_benchmark`, never into a real connector's table.
BENCH_CONNECTOR = "benchmark"

BENCH_ID_PREFIX = "bench-"

# One pre-built pool of random characters, sliced per record. Building this
# once keeps generation cost out of the measured section, and random (rather
# than repeated) bytes stop Postgres from over-compressing the JSON payload.
_POOL_SIZE = 8192
_FILLER_POOL = "".join(
    random.Random(0).choice(string.ascii_letters + string.digits) for _ in range(_POOL_SIZE)
) * 2

_LANGUAGES = ["Python", "Go", "Rust", "TypeScript", "Java"]


def _filler(index: int, size: int) -> str:
    if size <= 0:
        return ""
    if size > _POOL_SIZE:
        raise ValueError(f"payload_bytes must be <= {_POOL_SIZE}")
    start = (index * 97) % _POOL_SIZE
    return _FILLER_POOL[start:start + size]


def make_records(
    n: int,
    error_rate: float = 0.0,
    payload_bytes: int = 384,
    seed: int = 1234,
    id_offset: int = 0,
    null_description_every: int = 10,
) -> List[Dict[str, Any]]:
    """Build `n` raw GitHub-shaped records.

    `error_rate` is the fraction given an unparseable `created_at`, which makes
    `CommonData` validation raise and sends the record down the dead-letter
    path — the same failure mode a malformed upstream payload produces.

    `id_offset` shifts the id range, so a caller can generate a batch that is
    entirely new to a table (or one that deliberately collides with it).
    Every `null_description_every`-th record omits `description` so the NULL
    path is exercised rather than assumed.
    """
    rng = random.Random(seed)

    bad_indexes = set()
    n_bad = int(round(n * error_rate))
    if n_bad:
        bad_indexes = set(rng.sample(range(n), n_bad))

    records = []
    for i in range(n):
        key = i + id_offset
        record = {
            "id": f"{BENCH_ID_PREFIX}{key:09d}",
            "name": f"repo-{key:09d}",
            "description": f"Synthetic repository {key} generated for pipeline benchmarking",
            "created_at": "not-a-timestamp" if i in bad_indexes else "2024-01-15T10:30:00Z",
            "stargazers_count": rng.randint(0, 50000),
            "forks_count": rng.randint(0, 5000),
            "open_issues_count": rng.randint(0, 500),
            "language": rng.choice(_LANGUAGES),
            "topics": ["etl", "benchmark", rng.choice(_LANGUAGES).lower()],
            "payload": _filler(i, payload_bytes),
        }
        if null_description_every and key % null_description_every == 0:
            del record["description"]
        records.append(record)
    return records


def make_nasty_records() -> List[Dict[str, Any]]:
    """Records whose values would break a naive COPY encoder.

    Tabs and newlines are the field and record separators in COPY text format,
    and backslash is its escape character, so these are the values that decide
    whether the fast load path is actually equivalent to the slow one.
    """
    values = [
        "tab\tseparated",
        "newline\nin\nthe\nmiddle",
        "carriage\r\nreturn",
        "back\\slash",
        "double\\\\backslash",
        "literal backslash-N: \\N",
        'quotes "double" and \'single\'',
        "comma,semicolon;pipe|",
        "unicode: café — 日本語 — 🚀",
        "trailing whitespace   ",
        "",
    ]
    records = []
    for i, value in enumerate(values):
        records.append({
            "id": f"{BENCH_ID_PREFIX}nasty-{i:03d}",
            "name": value,
            "description": None if i % 3 == 0 else value,
            "created_at": "2024-01-15T10:30:00Z",
            "payload": value,
            "nested": {"key": value, "list": [value, 1, None]},
        })
    return records


def mean_record_bytes(records: List[Dict[str, Any]], sample: int = 200) -> float:
    """Mean compact-JSON size of a record, so throughput can be reported per byte."""
    take = records[:sample] or records
    if not take:
        return 0.0
    return sum(len(json.dumps(r, separators=(",", ":"))) for r in take) / len(take)
