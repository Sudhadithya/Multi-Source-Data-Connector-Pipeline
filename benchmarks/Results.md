# Transform + Load Benchmarks

The pipeline splits cleanly in two. The **connector layer** is network-bound
against third-party APIs — timing it measures GitHub's infrastructure, not this
code, so it is covered by correctness tests against mocked HTTP instead. The
**transform + load** path is fully local and deterministic, so that is what gets
measured here.

Three changes came out of these measurements. Each is documented below as
*what it was → what changed → what it measured*.

| change | effect |
|---|---|
| Batched the dead-letter writes | **25×** transform throughput at a 10% error rate |
| `COPY` into a staging table instead of `INSERT` | **5.4×** load throughput, byte-identical output |
| Chunked the multi-row `INSERT` | 1.18×, and bounds statement size |

## Running

```bash
# Isolated benchmark database (leaves the dev database in docker-compose.yml alone)
docker compose -p msdcp-bench -f benchmarks/docker-compose.bench.yml up -d

DATABASE_URL=postgresql://postgres:benchpass@localhost:5434/bench_db \
    python -m benchmarks.run_all --records 50000 --runs 5 --label baseline

# Single phase
DATABASE_URL=... python -m benchmarks.run_all --only strategy --only steady --label copy
```

Results are written to `benchmarks/results/<label>.json`. Benchmark rows go to
`data_benchmark` and to `failed_records` under ids prefixed `bench-`, so nothing
else in the database is touched.

## Method

- Synthetic GitHub-shaped records (`benchmarks/generate.py`), **~675 bytes each**
  as compact JSON. They flow through the real `Transformer.transform` branch,
  not a benchmark-only path. Every 10th record omits `description` so the NULL
  path is exercised rather than assumed.
- Malformed records are injected by making `created_at` unparseable, so
  `CommonData` validation raises — the same failure mode a bad upstream payload
  produces.
- Every number is the **median of 5 runs** (dead-letter sweep: 2), with the
  target table reset before each run. Run-to-run spread is **5–10%**, so
  differences under ~10% are noise. Head-to-head comparisons are always taken
  from the *same* run, never across runs.
- One connection, sequential, no concurrency.

### Environment

| | |
|---|---|
| Host | Windows 11, 20 logical CPUs |
| Postgres | 17.9 (Debian) in Docker Desktop, named volume |
| PG settings | `synchronous_commit=on`, `fsync=on`, `shared_buffers=128MB` (all defaults — the server is deliberately untuned) |
| Python | 3.12.5, SQLAlchemy 2.0.23, psycopg2 2.9.9 |

Single-machine numbers on Docker Desktop for Windows, where container storage
goes through a virtualized filesystem — a write-heavy Postgres workload is
exactly what that penalizes. Absolute throughput will be higher on native Linux;
the **ratios** are the portable part.

---

## 1. Where the time goes

50,000 records, median of 5.

| stage | throughput |
|---|---:|
| transform only (no database) | **110,933 rec/s** |
| load (original) | 6,948 rec/s |
| load (current) | **37,702 rec/s** |

Transform was never the bottleneck — it is ~15× faster than the original load
path. Every change below targets the database side.

---

## 2. Change: batch the dead-letter writes

### Before

`Transformer.transform` isolates each record in its own `try/except` and
dead-letters failures. The handler it called, `insert_failed_record`, opened its
own session, inserted **one row, and committed** — per bad record. A batch with
10% malformed records did 2,000 separate transactions, and each commit must
flush the write-ahead log and wait for it. That cost is nearly identical whether
the transaction carries 1 row or 5,000.

### Changed

Failures are buffered during the loop and written once at the end via
`insert_failed_records`, in chunks of 500 inside a single transaction. Per-record
`try/except` and log-and-continue are unchanged — **only the write is batched**.

### After

20,000 records per point, transform only (load excluded to isolate the cost):

| malformed | failures | before (rec/s) | after (rec/s) | speedup |
|---:|---:|---:|---:|---:|
| 0% | 0 | 107,730 | 128,209 | — (noise) |
| 1% | 200 | 17,390 | 91,445 | **5.3×** |
| 5% | 1,000 | 3,819 | 58,268 | **15.3×** |
| 10% | 2,000 | 1,909 | 48,242 | **25.3×** |
| 25% | 5,000 | 774 | 20,639 | **26.7×** |

At 10% errors, transforming 20,000 records went from **10.5 s to 0.42 s** — and
over 98% of that original time was commit overhead on the error path, not work.
A 1% error rate, entirely ordinary for a third-party feed, already cost 6×.

What the batched writer preserves:

- Duplicate `(id, source)` keys are collapsed before the statement is built.
  Postgres rejects an `ON CONFLICT DO UPDATE` that touches the same row twice,
  and records with no id fall back to a timestamp-derived id that can collide.
- If the batch is rejected, each row is retried individually, so one poisonous
  record cannot discard the rest.
- It never raises. The dead-letter table is itself the error path.

Trade-off: dead-letter rows become durable at the end of the transform batch
rather than immediately, so a hard crash mid-transform loses the buffered
failures. For a sync that re-runs idempotently, that is worth 25×.

---

## 3. Change: chunk the multi-row INSERT

### Before

`upsert_data` was **already batched** — it always emitted a single multi-row
`INSERT ... ON CONFLICT` for the whole list. There was no row-at-a-time version,
so there is no dramatic before/after here. What it did have was an unbounded
statement: 50,000 records meant one statement carrying 50,000 rows.

### Changed

`chunk_size` (default 1000) splits the rows across statements, still inside one
transaction with one commit.

### After

50,000 records into an empty table, median of 5:

| rows/statement | median s | rec/s |
|---:|---:|---:|
| 250 | 12.819 | 3,900 |
| 500 | 8.709 | 5,741 |
| **1000** | **6.784** | **7,370** |
| 2500 | 6.982 | 7,162 |
| 5000 | 6.831 | 7,320 |
| 10000 | 7.124 | 7,018 |
| single statement (50k rows) | 7.986 | 6,261 |

Worth **1.18×**, and 1000–5000 is a single plateau — differences inside it are
within noise. The honest argument for chunking is not the 18%: it bounds
statement size, so a sync of any size emits statements of constant size.

The 50k single statement did **not** hit Postgres's 65535 bind-parameter ceiling.
psycopg2 interpolates parameters client-side, so that limit does not apply here;
the statement just gets slower to build.

---

## 4. Change: COPY into a staging table

This is the change that mattered.

### Before

Even chunked, a multi-row `INSERT` pays per-row overhead through the extended
query protocol, and every row probes the `(id, source)` index as it arrives.

### Changed

A second strategy, `strategy="copy"`, now the default:

1. `CREATE TEMP TABLE ... (LIKE data_x) ON COMMIT DROP` — no indexes, and temp
   tables write no WAL for their contents.
2. `COPY ... FROM STDIN` streams every row in one operation.
3. One `INSERT ... SELECT DISTINCT ON (id, source) ... ON CONFLICT DO UPDATE`
   merges staging into the real table.

`DISTINCT ON` collapses duplicate keys inside the batch, and ordering the select
by `(id, source)` groups index writes instead of scattering them. Both strategies
remain available; `multirow` is one argument away.

### After — empty table

50,000 records, median of 5, same run:

| strategy | median s | rec/s | |
|---|---:|---:|---|
| multirow (chunk 1000) | 7.196 | 6,948 | |
| **copy** | **1.326** | **37,702** | **5.4×** |

### After — small batches

Real connectors return tens or hundreds of records, so COPY's fixed overhead
(two extra statements) had to be checked before making it the default:

| records | multirow rec/s | copy rec/s |
|---:|---:|---:|
| 50 | 945 | 1,000 |
| 200 | 3,190 | 3,728 |
| 1,000 | 8,207 | 14,437 |
| 5,000 | 7,891 | 27,058 |

COPY is never slower. At 50–200 records both are dominated by the single
commit's fsync, so the strategy barely matters; above ~1,000 COPY pulls away.

### Parity — is it actually equivalent?

A faster loader is only interesting if it stores the same thing. The `parity`
phase loads identical records both ways and compares an md5 digest of every row,
including 11 deliberately hostile values — tabs, newlines, CRLF, single and
double backslashes, a literal `\N`, quotes, unicode, and empty strings. Tab and
newline are COPY's field and record separators and backslash is its escape
character, so these are exactly the values a naive encoder corrupts.

| | |
|---|---|
| rows match | **true** (5,011 / 5,011) |
| byte-identical digest | **true** |
| json-normalized digest | **true** |

---

## 5. Measurement fix: benchmark against a non-empty table

### Before

Every load benchmark truncated the table first. That is the flattering case: a
shallow index and nothing to conflict with. Real syncs run against a table that
has been accumulating for months.

### Changed

The `steady` phase preloads **500,000 rows (454 MB)** and then measures two
distinct workloads — a batch of entirely new ids, and a batch where every row
collides with an existing one. Each run of the new-rows workload uses its own id
range, so no run is warmed up by the previous one.

### After

50,000 records per load, into a table holding 500k–1M rows:

| strategy | workload | median s | rec/s | vs empty table |
|---|---|---:|---:|---:|
| multirow | new rows | 7.974 | 6,270 | −10% |
| multirow | existing rows | 8.703 | 5,745 | −17% |
| copy | new rows | 1.440 | 34,721 | −8% |
| **copy** | **existing rows** | **2.041** | **24,499** | **−35%** |

**The empty-table benchmark overstates throughput by 8–35%.** The worst case is
the one that matters most in production — re-syncing rows that already exist —
and it is also where COPY's advantage narrows, from 5.4× down to **4.3×**.
Publishing the empty-table number alone would have been the most flattering and
least useful figure available.

---

## 6. Idempotency

Not a benchmark — a demonstration, re-run under the current default (COPY).
50,000 records loaded, then the same 50,000 primary keys loaded again with a
modified `title`.

| | |
|---|---|
| first load | 1.159 s → 50,000 rows |
| second load | 1.400 s → **50,000 rows** |
| distinct `(id, source)` | 50,000 |
| rows carrying the second load's value | **50,000 / 50,000** |

Re-running a sync neither duplicates rows nor fails on them. Row count alone
would also be consistent with the second write doing nothing, so the second pass
changes every `title` and the check confirms all 50,000 rows carry the new value
— `ON CONFLICT DO UPDATE`, demonstrated rather than asserted.

The second load is ~21% slower: it is an all-conflict pass, paying index
maintenance and dead-tuple cost that an insert into an empty table does not.

---

## What is still open

- **Native Linux numbers.** Everything here runs through Docker Desktop for
  Windows. The ratios should hold; the absolute figures should improve.
- **Concurrency.** All measurements use one connection. Parallel loaders would
  need batches sorted by key to avoid deadlocking on overlapping upserts — the
  COPY path already sorts, which is a prerequisite rather than a solution.
- **Payload size sweep.** Everything here is ~675 bytes/record. Throughput in
  rec/s is only meaningful at a stated row size.
