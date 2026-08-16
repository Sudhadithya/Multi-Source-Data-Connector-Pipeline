# Architecture

A FastAPI service that syncs data from multiple third-party APIs into Postgres
through a strict 3-layer ETL pipeline. `POST /sync/{connector_name}` schedules
`run_sync_pipeline()` in [`app/main.py`](../app/main.py) as a background task,
which drives all three layers in order.

```
Connector (app/connectors/) → Transform (app/transform/) → Loader (app/loader/)
     raw dicts                    CommonData                  Postgres upsert
```

The layers are deliberately not collapsed. Each owns a distinct failure-handling
responsibility, described under [Design invariants](#design-invariants).

## 1. Connector layer — `app/connectors/`

Pulls raw, source-shaped JSON from an external API. One file per source:

- [`base.py`](../app/connectors/base.py) — the `BaseConnector` abstract base class
- [`factory.py`](../app/connectors/factory.py) — `ConnectorFactory.get_connector(name)`, a hardcoded `if/elif` mapping connector name → class
- [`github.py`](../app/connectors/github.py), [`basic_auth.py`](../app/connectors/basic_auth.py), [`stripe.py`](../app/connectors/stripe.py), [`newsapi.py`](../app/connectors/newsapi.py), [`openweathermap.py`](../app/connectors/openweathermap.py) — one concrete connector each

Connectors know nothing about `CommonData` or Postgres. They return
`List[Dict[str, Any]]` in whatever shape the source API produces.

Rate-limited connectors (`github.py`, `basic_auth.py`) wrap their HTTP call in a
`_make_request` helper decorated with `tenacity.retry` (exponential backoff, 5
attempts) that raises a local `RateLimitException` on HTTP 429. New connectors
that can be rate-limited should follow this pattern rather than introducing a
second retry mechanism.

## 2. Transform layer — `app/transform/transformer.py`

`Transformer.transform(source, raw_records) -> List[CommonData]` maps each
source's raw shape into the common schema. There is an explicit `if/elif` branch
per `source` string, keyed on the same name used in `config.yaml` and the
factory.

## 3. Loader layer — `app/loader/postgres.py`

`upsert_data(records, connector_name, chunk_size, strategy)` writes to a
**dynamically named table per source** — `data_<connector_name>`, created on
first use by `get_dynamic_table` with `checkfirst=True`.

Two load strategies, both running in **one transaction with a single commit**
and both keyed on `(id, source)`:

| strategy | mechanism | notes |
|---|---|---|
| `"copy"` (default) | `_upsert_copy` streams rows into an unindexed `TEMP` staging table (`ON COMMIT DROP`) via `COPY ... FROM STDIN`, then merges once with `INSERT ... SELECT DISTINCT ON (id, source) ... ON CONFLICT DO UPDATE` | **5.4× faster** than `multirow` and byte-identical |
| `"multirow"` | `_upsert_multirow` issues chunked `INSERT ... ON CONFLICT` statements, `chunk_size` rows each (default 1000; `None` emits one statement for the whole batch) | kept as a fallback and as the comparison baseline |

`COPY` is delimiter-sensitive, so values containing tabs, newlines, or
backslashes have to be escaped explicitly. `_copy_field` owns that escaping, and
the `parity` benchmark phase is what proves the two strategies still store
byte-identical data — keep it passing when touching that function.

This layer also owns the dead-letter table, `failed_records`:
`insert_failed_records(source, failures)` for a batch of
`(raw_data, error_reason)` pairs, with `insert_failed_record(...)` as the
single-record wrapper.

## Common schema

Every connector's output is normalized into `CommonData`
([`app/models/schema.py`](../app/models/schema.py)):

```python
class CommonData(BaseModel):
    id: str
    source: str
    title: str
    description: Optional[str] = None
    created_at: datetime
    raw_data: Dict[str, Any]
```

## Supporting modules

- [`app/config.py`](../app/config.py) — `Settings` (pydantic-settings). `settings.connectors_config` re-reads `app/config.yaml` on every access and injects secrets from env vars (`GITHUB_TOKEN`, `STRIPE_API_KEY`, …) into the per-connector dict before it reaches the connector's `__init__`.
- [`app/config.yaml`](../app/config.yaml) — non-secret per-connector settings only (base URLs, repo lists, query lists, city lists). API keys and tokens belong in `.env` and are merged in by `config.py`.
- [`app/utils/logger.py`](../app/utils/logger.py) — `get_logger(name)`, stdlib logging to stdout.

## Adding a connector

All five steps are required together; they are not independently discoverable.

1. Create `app/connectors/<name>.py` with a class implementing `authenticate`, `fetch_data`, and `handle_pagination`.
2. Register it in the `if/elif` chain in `ConnectorFactory.get_connector` ([`factory.py`](../app/connectors/factory.py)).
3. Add a matching entry under `connectors:` in [`app/config.yaml`](../app/config.yaml), using the exact key used in the factory and transformer.
4. Add a branch for that source name in `Transformer.transform` ([`transformer.py`](../app/transform/transformer.py)) mapping its raw shape to `CommonData`.
5. If the connector needs a secret, add a field to `Settings` in `app/config.py`, wire it into `connectors_config`, and document it in `.env.example`.

The `BaseConnector` contract is three abstract methods:

```python
class BaseConnector(ABC):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.session = self.authenticate()   # called automatically

    @abstractmethod
    def authenticate(self):
        """Build and return a requests.Session (or equivalent), applying auth headers/params."""

    @abstractmethod
    def fetch_data(self) -> List[Dict[str, Any]]:
        """Fetch and return all raw records for this source as a flat list of dicts."""

    @abstractmethod
    def handle_pagination(self, *args, **kwargs):
        """Walk pages/cursors for one endpoint/query and return the combined list of raw items."""
```

Existing connectors cover the common auth styles: `github.py` (Bearer token +
Link-header pagination), `basic_auth.py` (HTTP basic auth), `stripe.py` (Bearer
token + `starting_after` cursor pagination), and `newsapi.py` /
`openweathermap.py` (API key in header or query param).

## Design invariants

These are load-bearing. Each exists because the alternative was measured or
observed to be worse.

**Upserts stay idempotent.** `upsert_data` and `insert_failed_record` both use
Postgres `INSERT ... ON CONFLICT (id, source) DO UPDATE`, never a plain
`INSERT`. The composite primary key is always `(id, source)`. Re-running a sync
for the same connector must update existing rows rather than duplicating them or
failing. Changes to loader code need to keep the `on_conflict_do_update` clause
and keep `id`+`source` as the conflict target.

**Transform failures are isolated per record.** In `Transformer.transform`, each
raw record is transformed inside its own `try/except`. On exception the record is
appended to a `failures` buffer and the loop *continues* — one bad record must
never abort the batch or crash the sync. This should not become a batch-level
`try/except`.

**Only the dead-letter *write* is batched, never the error handling.** The
failures buffer is flushed once after the loop via `insert_failed_records`.
Committing one transaction per bad record cost 25× throughput at a 10% error
rate. `insert_failed_records` keeps three safety properties: it dedupes
`(id, source)` within the batch (Postgres rejects an `ON CONFLICT DO UPDATE`
touching the same row twice), it falls back to per-row writes if the batch is
rejected, and it never raises — the dead-letter table is itself the error path.

**Connectors log and continue too.** `fetch_data()` methods wrap each unit of
work (per repo, per query, per city) in its own `try/except`, so one failing item
does not kill the whole call.

**Table naming is fixed.** Data tables are `data_<connector_name>` (sanitized,
lowercased); the dead-letter table is always `failed_records`.

## Testing

Run with `pytest tests/`. `pytest.ini` sets `pythonpath = .`, so no `PYTHONPATH`
juggling is needed.

**The database is always mocked, never real.**
[`tests/test_loader.py`](../tests/test_loader.py) patches
`app.loader.postgres.SessionLocal` and `Table.create`; no test opens a real
Postgres connection. New loader tests should follow the same pattern.

[`tests/test_transformer.py`](../tests/test_transformer.py) tests
`Transformer.transform` directly as a pure function — one test per source shape,
plus unknown-source and missing-field cases. Adding a transformer branch means
adding a test here.

Tests assert on both the happy path and on `db.rollback()` / `db.commit()` call
state for failure paths. A failure must always roll back and never commit (see
`test_upsert_data_exception`).

## Benchmarks

`benchmarks/` measures the local half of the pipeline (transform + load); the
connector layer is network-bound and is covered by correctness tests instead. It
runs against its own throwaway Postgres on port 5434
([`docker-compose.bench.yml`](../benchmarks/docker-compose.bench.yml)) — never
the dev database in `docker-compose.yml`.

```bash
docker compose -p msdcp-bench -f benchmarks/docker-compose.bench.yml up -d

DATABASE_URL=postgresql://postgres:benchpass@localhost:5434/bench_db \
    python -m benchmarks.run_all --records 50000 --runs 5 --label baseline
```

Seven phases, selectable with repeated `--only`: `transform` (no DB), `load`
(rows-per-`INSERT` sweep), `idempotency` (same keys twice — row count unchanged,
every row updated), `dlq` (throughput vs error rate), `parity` (COPY and multirow
store byte-identical data), `strategy` (COPY vs multirow), and `steady` (loads
against a table preloaded with `--preload` rows, since an empty table overstates
throughput by 8–35%).

Results land in `benchmarks/results/<label>.json`. Methodology, environment, and
current numbers are in [`benchmarks/Results.md`](../benchmarks/Results.md).
Benchmark rows are confined to `data_benchmark` and to `failed_records` ids
prefixed `bench-`.

If you change loader or transform internals, re-run the suite and update the
numbers rather than leaving stale figures in place.
