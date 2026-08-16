"""Benchmark runner for the local half of the pipeline (transform + load).

The connector layer is deliberately excluded: it is network-bound against
third-party APIs, so its throughput measures someone else's infrastructure.
Everything measured here runs against Postgres 17 from docker-compose.yml.

Usage:
    python -m benchmarks.run_all --label baseline
    python -m benchmarks.run_all --label batched-dlq --only dlq
"""

import argparse
import json
import logging
import os
import time
from datetime import timezone
from typing import Any, Dict, List

from app.loader.postgres import init_db, upsert_data
from app.transform.transformer import Transformer
from benchmarks.generate import (
    BENCH_CONNECTOR,
    BENCH_SOURCE,
    make_nasty_records,
    make_records,
    mean_record_bytes,
)
from benchmarks.harness import (
    BENCH_TABLE,
    bench_dlq_count,
    env_info,
    repeat,
    reset_bench_dlq,
    reset_bench_table,
    row_count,
    table_digest,
    table_size_pretty,
    throughput,
    timeit,
)

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")

CHUNK_SIZES = [250, 500, 1000, 2500, 5000, 10000, None]
ERROR_RATES = [0.0, 0.01, 0.05, 0.10, 0.25]


def bench_transform(n: int, runs: int, payload_bytes: int) -> Dict[str, Any]:
    """Transform-only throughput, no database involved (0% malformed records)."""
    raw = make_records(n, error_rate=0.0, payload_bytes=payload_bytes)
    transformer = Transformer()

    result = repeat(lambda: transformer.transform(BENCH_SOURCE, raw), runs=runs)
    result["records"] = n
    result["records_per_s"] = throughput(n, result["median_s"])
    result["mean_record_bytes"] = mean_record_bytes(raw)
    return result


def bench_load_sweep(n: int, runs: int, payload_bytes: int) -> Dict[str, Any]:
    """Load throughput vs rows-per-INSERT, into an empty table each run."""
    raw = make_records(n, error_rate=0.0, payload_bytes=payload_bytes)
    records = Transformer().transform(BENCH_SOURCE, raw)

    points: List[Dict[str, Any]] = []
    for chunk in CHUNK_SIZES:
        label = "single statement" if chunk is None else str(chunk)
        point: Dict[str, Any] = {"chunk_size": chunk, "label": label}
        try:
            stats = repeat(
                lambda c=chunk: upsert_data(records, BENCH_CONNECTOR, chunk_size=c),
                runs=runs,
                setup=reset_bench_table,
            )
            point.update(stats)
            point["records"] = len(records)
            point["records_per_s"] = throughput(len(records), stats["median_s"])
            point["rows_loaded"] = row_count(BENCH_TABLE)
        except Exception as exc:  # a chunk size that cannot run is itself a result
            point["error"] = f"{type(exc).__name__}: {exc}"[:300]
        points.append(point)

    return {
        "records": len(records),
        "mean_record_bytes": mean_record_bytes(raw),
        "points": points,
        "table_size_after": table_size_pretty(BENCH_TABLE),
    }


def bench_idempotency(n: int, payload_bytes: int, chunk_size: int) -> Dict[str, Any]:
    """Load the same keys twice and show the row count is unchanged and the
    second write actually updated the rows (ON CONFLICT DO UPDATE, not DO NOTHING).
    """
    raw = make_records(n, error_rate=0.0, payload_bytes=payload_bytes)
    records = Transformer().transform(BENCH_SOURCE, raw)

    reset_bench_table()

    first_s = timeit(lambda: upsert_data(records, BENCH_CONNECTOR, chunk_size=chunk_size))
    count_after_first = row_count(BENCH_TABLE)

    # Same primary keys, different payload: proves the second pass updates.
    for r in records:
        r.title = f"{r.title} (v2)"

    second_s = timeit(lambda: upsert_data(records, BENCH_CONNECTOR, chunk_size=chunk_size))
    count_after_second = row_count(BENCH_TABLE)

    from benchmarks.harness import scalar
    updated = int(scalar(f"SELECT count(*) FROM {BENCH_TABLE} WHERE right(title, 5) = ' (v2)'"))
    distinct_keys = int(scalar(f"SELECT count(DISTINCT (id, source)) FROM {BENCH_TABLE}"))

    return {
        "records": len(records),
        "chunk_size": chunk_size,
        "first_load_s": first_s,
        "second_load_s": second_s,
        "first_load_records_per_s": throughput(len(records), first_s),
        "second_load_records_per_s": throughput(len(records), second_s),
        "rows_after_first": count_after_first,
        "rows_after_second": count_after_second,
        "rows_unchanged": count_after_first == count_after_second,
        "rows_updated_to_v2": updated,
        "all_rows_updated": updated == count_after_second,
        "distinct_keys": distinct_keys,
        "table_size_after": table_size_pretty(BENCH_TABLE),
    }


def bench_parity(payload_bytes: int) -> Dict[str, Any]:
    """Prove the COPY path stores exactly what the INSERT path stores.

    A faster loader is only interesting if it is equivalent, so this loads the
    same records both ways — including values containing tabs, newlines and
    backslashes, which are COPY's separators and escape character — and
    compares a digest of the resulting tables.
    """
    raw = make_records(5000, payload_bytes=payload_bytes) + make_nasty_records()
    records = Transformer().transform(BENCH_SOURCE, raw)

    reset_bench_table()
    upsert_data(records, BENCH_CONNECTOR, strategy="multirow", chunk_size=1000)
    multirow_digest = table_digest(BENCH_TABLE)
    multirow_json_digest = table_digest(BENCH_TABLE, normalize_json=True)
    multirow_rows = row_count(BENCH_TABLE)

    reset_bench_table()
    upsert_data(records, BENCH_CONNECTOR, strategy="copy")
    copy_digest = table_digest(BENCH_TABLE)
    copy_json_digest = table_digest(BENCH_TABLE, normalize_json=True)
    copy_rows = row_count(BENCH_TABLE)

    return {
        "records": len(records),
        "nasty_records": len(make_nasty_records()),
        "multirow_rows": multirow_rows,
        "copy_rows": copy_rows,
        "rows_match": multirow_rows == copy_rows,
        "digest_match": multirow_digest == copy_digest,
        "json_normalized_digest_match": multirow_json_digest == copy_json_digest,
        "multirow_digest": multirow_digest,
        "copy_digest": copy_digest,
    }


def bench_strategy_compare(n: int, runs: int, payload_bytes: int) -> Dict[str, Any]:
    """multirow vs copy, loading into an empty table."""
    raw = make_records(n, payload_bytes=payload_bytes)
    records = Transformer().transform(BENCH_SOURCE, raw)

    points = []
    for strategy, chunk in (("multirow", 1000), ("copy", None)):
        stats = repeat(
            lambda s=strategy, c=chunk: upsert_data(
                records, BENCH_CONNECTOR, chunk_size=c, strategy=s
            ),
            runs=runs,
            setup=reset_bench_table,
        )
        points.append({
            "strategy": strategy,
            **stats,
            "records": len(records),
            "records_per_s": throughput(len(records), stats["median_s"]),
            "rows_loaded": row_count(BENCH_TABLE),
        })
    return {"records": len(records), "points": points}


def bench_steady_state(n: int, runs: int, payload_bytes: int, preload: int) -> Dict[str, Any]:
    """The same load against a table that already holds `preload` rows.

    Loading into an empty table is the flattering case: the index is shallow and
    there is nothing to conflict with. Real syncs run against a table that has
    been accumulating for months, so this measures both halves of that — rows
    that are new, and rows that all collide.
    """
    reset_bench_table()

    # Preload is setup, not measurement. Built in slices so half a million
    # records never sit in memory at once.
    preload_slice = 50000
    preload_s = 0.0
    for start in range(0, preload, preload_slice):
        count = min(preload_slice, preload - start)
        batch = Transformer().transform(
            BENCH_SOURCE,
            make_records(count, payload_bytes=payload_bytes, id_offset=start),
        )
        preload_s += timeit(
            lambda b=batch: upsert_data(b, BENCH_CONNECTOR, strategy="copy")
        )

    rows_before = row_count(BENCH_TABLE)
    size_before = table_size_pretty(BENCH_TABLE)

    # All-existing rows: ids drawn from the preloaded range, so every row
    # conflicts and every write is an update.
    existing_batch = Transformer().transform(
        BENCH_SOURCE, make_records(n, payload_bytes=payload_bytes, id_offset=0)
    )

    # All-new rows: every run gets its own id range, so no run is warmed up by
    # the previous one and no strategy is handed rows another already inserted.
    next_offset = preload

    points = []
    for strategy, chunk in (("multirow", 1000), ("copy", None)):
        for workload in ("new_rows", "existing_rows"):
            durations = []
            for _ in range(runs):
                if workload == "new_rows":
                    batch = Transformer().transform(
                        BENCH_SOURCE,
                        make_records(n, payload_bytes=payload_bytes, id_offset=next_offset),
                    )
                    next_offset += n
                else:
                    batch = existing_batch
                durations.append(
                    timeit(lambda b=batch, s=strategy, c=chunk: upsert_data(
                        b, BENCH_CONNECTOR, chunk_size=c, strategy=s
                    ))
                )
            median = sorted(durations)[len(durations) // 2]
            points.append({
                "strategy": strategy,
                "workload": workload,
                "runs": runs,
                "median_s": median,
                "min_s": min(durations),
                "max_s": max(durations),
                "all_s": durations,
                "records": n,
                "records_per_s": throughput(n, median),
            })

    return {
        "records_per_load": n,
        "preload_rows": preload,
        "preload_s": preload_s,
        "rows_before": rows_before,
        "table_size_before": size_before,
        "rows_after": row_count(BENCH_TABLE),
        "table_size_after": table_size_pretty(BENCH_TABLE),
        "points": points,
    }


def bench_dlq_sweep(n: int, runs: int, payload_bytes: int) -> Dict[str, Any]:
    """Transform throughput vs share of malformed records.

    Exercises the dead-letter path in `Transformer.transform`. Load is excluded
    so the measurement isolates the cost of handling failures.
    """
    transformer = Transformer()
    points: List[Dict[str, Any]] = []

    for rate in ERROR_RATES:
        raw = make_records(n, error_rate=rate, payload_bytes=payload_bytes)
        expected_bad = int(round(n * rate))

        stats = repeat(
            lambda r=raw: transformer.transform(BENCH_SOURCE, r),
            runs=runs,
            setup=reset_bench_dlq,
        )
        written = bench_dlq_count()

        points.append({
            "error_rate": rate,
            "records": n,
            "expected_failures": expected_bad,
            "dlq_rows_written": written,
            "dlq_rows_match": written == expected_bad,
            **stats,
            "records_per_s": throughput(n, stats["median_s"]),
        })

    reset_bench_dlq()
    return {"records": n, "points": points}


def _fmt(v: Any) -> str:
    if isinstance(v, float):
        return f"{v:,.1f}"
    return str(v)


def print_report(results: Dict[str, Any]) -> None:
    print()
    print("=" * 78)
    print("MULTI-SOURCE PIPELINE — TRANSFORM + LOAD BENCHMARKS")
    print("=" * 78)

    env = results["environment"]
    print(f"  label            : {results['label']}")
    print(f"  python           : {env['python']}")
    print(f"  platform         : {env['platform']}")
    print(f"  cpu_count        : {env['cpu_count']}")
    print(f"  postgres         : {env['postgres']}")
    print(f"  pg settings      : {env['postgres_settings']}")
    print(f"  payload/record   : {results.get('mean_record_bytes', 0):,.0f} bytes (compact JSON)")

    if "transform" in results:
        t = results["transform"]
        print()
        print("-- 1. TRANSFORM ONLY (no database) " + "-" * 43)
        print(f"  {t['records']:,} records, median of {t['runs']} runs")
        print(f"  median {t['median_s']:.3f}s   ->   {t['records_per_s']:,.0f} rec/s")

    if "load" in results:
        print()
        print("-- 2. LOAD: ROWS PER INSERT STATEMENT " + "-" * 40)
        ld = results["load"]
        print(f"  {ld['records']:,} records into an empty table, single transaction")
        print(f"  {'chunk':>16}  {'median s':>10}  {'rec/s':>12}  {'rows':>8}")
        for p in ld["points"]:
            if "error" in p:
                print(f"  {p['label']:>16}  {'FAILED':>10}  {p['error'][:40]}")
            else:
                print(f"  {p['label']:>16}  {p['median_s']:>10.3f}  "
                      f"{p['records_per_s']:>12,.0f}  {p['rows_loaded']:>8,}")

    if "idempotency" in results:
        i = results["idempotency"]
        print()
        print("-- 3. IDEMPOTENCY (same keys loaded twice) " + "-" * 35)
        print(f"  first  load : {i['first_load_s']:.3f}s  "
              f"({i['first_load_records_per_s']:,.0f} rec/s)  -> {i['rows_after_first']:,} rows")
        print(f"  second load : {i['second_load_s']:.3f}s  "
              f"({i['second_load_records_per_s']:,.0f} rec/s)  -> {i['rows_after_second']:,} rows")
        print(f"  row count unchanged      : {i['rows_unchanged']}")
        print(f"  all rows updated to v2   : {i['all_rows_updated']}  "
              f"({i['rows_updated_to_v2']:,}/{i['rows_after_second']:,})")
        print(f"  distinct (id, source)    : {i['distinct_keys']:,}")

    if "parity" in results:
        p = results["parity"]
        print()
        print("-- 5. STRATEGY PARITY (copy vs multirow store the same data) " + "-" * 17)
        print(f"  {p['records']:,} records, including {p['nasty_records']} hostile values")
        print(f"  rows match               : {p['rows_match']}  "
              f"({p['multirow_rows']:,} / {p['copy_rows']:,})")
        print(f"  byte-identical digest    : {p['digest_match']}")
        print(f"  json-normalized digest   : {p['json_normalized_digest_match']}")

    if "strategy" in results:
        print()
        print("-- 6. LOAD STRATEGY, EMPTY TABLE " + "-" * 45)
        s = results["strategy"]
        print(f"  {s['records']:,} records into an empty table")
        print(f"  {'strategy':>16}  {'median s':>10}  {'rec/s':>12}  {'rows':>10}")
        for p in s["points"]:
            print(f"  {p['strategy']:>16}  {p['median_s']:>10.3f}  "
                  f"{p['records_per_s']:>12,.0f}  {p['rows_loaded']:>10,}")

    if "steady" in results:
        print()
        print("-- 7. LOAD STRATEGY, NON-EMPTY TABLE " + "-" * 41)
        st = results["steady"]
        print(f"  preloaded {st['preload_rows']:,} rows ({st['table_size_before']}) "
              f"in {st['preload_s']:.1f}s")
        print(f"  {st['records_per_load']:,} records per load; "
              f"table grew to {st['rows_after']:,} rows ({st['table_size_after']})")
        print(f"  {'strategy':>16}  {'workload':>15}  {'median s':>10}  {'rec/s':>12}")
        for p in st["points"]:
            print(f"  {p['strategy']:>16}  {p['workload']:>15}  "
                  f"{p['median_s']:>10.3f}  {p['records_per_s']:>12,.0f}")

    if "dlq" in results:
        print()
        print("-- 4. DEAD-LETTER PATH: THROUGHPUT vs ERROR RATE " + "-" * 29)
        d = results["dlq"]
        print(f"  {d['records']:,} records transformed per point")
        print(f"  {'bad %':>8}  {'failures':>9}  {'median s':>10}  {'rec/s':>12}  {'dlq ok':>7}")
        for p in d["points"]:
            print(f"  {p['error_rate'] * 100:>7.0f}%  {p['expected_failures']:>9,}  "
                  f"{p['median_s']:>10.3f}  {p['records_per_s']:>12,.0f}  {str(p['dlq_rows_match']):>7}")

    print()
    print("=" * 78)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, default=50000)
    parser.add_argument("--dlq-records", type=int, default=20000)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--dlq-runs", type=int, default=2)
    parser.add_argument("--payload-bytes", type=int, default=384)
    parser.add_argument("--idempotency-chunk", type=int, default=1000)
    parser.add_argument("--preload", type=int, default=500000,
                        help="Rows already in the table for the steady-state phase.")
    parser.add_argument("--label", default="baseline")
    parser.add_argument(
        "--only",
        choices=["transform", "load", "idempotency", "dlq", "parity", "strategy", "steady"],
        action="append",
        help="Run only these phases (repeatable). Default: all.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Keep application logging on (one line per dead-lettered record).",
    )
    args = parser.parse_args()

    if not args.verbose:
        # The dead-letter sweep logs per failed record; at 25% of 20k that is
        # 5,000 lines of noise, and the logging itself would distort timings.
        for name in ("transformer", "loader", "main"):
            logging.getLogger(name).setLevel(logging.CRITICAL)

    # The dead-letter path writes to `failed_records`; make sure it exists,
    # otherwise the sweep would time failing inserts instead of real ones.
    init_db()

    all_phases = ["transform", "load", "idempotency", "dlq", "parity", "strategy", "steady"]
    phases = args.only or all_phases

    results: Dict[str, Any] = {
        "label": args.label,
        "started_at": __import__("datetime").datetime.now(timezone.utc).isoformat(),
        "config": vars(args),
        "environment": env_info(),
        "mean_record_bytes": mean_record_bytes(make_records(200, payload_bytes=args.payload_bytes)),
    }

    if "transform" in phases:
        print("[1/7] transform-only ...", flush=True)
        results["transform"] = bench_transform(args.records, args.runs, args.payload_bytes)

    if "load" in phases:
        print("[2/7] load chunk sweep ...", flush=True)
        results["load"] = bench_load_sweep(args.records, args.runs, args.payload_bytes)

    if "idempotency" in phases:
        print("[3/7] idempotency ...", flush=True)
        results["idempotency"] = bench_idempotency(
            args.records, args.payload_bytes, args.idempotency_chunk
        )

    if "dlq" in phases:
        print("[4/7] dead-letter sweep ...", flush=True)
        results["dlq"] = bench_dlq_sweep(args.dlq_records, args.dlq_runs, args.payload_bytes)

    if "parity" in phases:
        print("[5/7] strategy parity ...", flush=True)
        results["parity"] = bench_parity(args.payload_bytes)

    if "strategy" in phases:
        print("[6/7] load strategy, empty table ...", flush=True)
        results["strategy"] = bench_strategy_compare(args.records, args.runs, args.payload_bytes)

    if "steady" in phases:
        print("[7/7] load strategy, non-empty table ...", flush=True)
        results["steady"] = bench_steady_state(
            args.records, args.runs, args.payload_bytes, args.preload
        )

    results["finished_at"] = __import__("datetime").datetime.now(timezone.utc).isoformat()

    os.makedirs(RESULTS_DIR, exist_ok=True)
    out_path = os.path.join(RESULTS_DIR, f"{args.label}.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, default=str)

    print_report(results)
    print(f"results written to {out_path}")


if __name__ == "__main__":
    start = time.perf_counter()
    main()
    print(f"total wall time: {time.perf_counter() - start:.1f}s")
