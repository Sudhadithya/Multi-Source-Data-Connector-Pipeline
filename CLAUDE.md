# Multi-Source Data Connector Pipeline

FastAPI service that syncs data from multiple third-party APIs into Postgres through a strict 3-layer ETL pipeline: Connector → Transform → Loader. Orchestrated by `app/main.py`, triggered via `POST /sync/{connector_name}`.

## Architecture

```
Connector (app/connectors/) → Transform (app/transform/) → Loader (app/loader/)
     raw dicts                    CommonData                  Postgres upsert
```

The three layers are wired together in [`run_sync_pipeline()`](app/main.py:62), called as a background task from `POST /sync/{connector_name}`. Never collapse or bypass the layers — each has a distinct failure-handling responsibility (see "Do not break" below).

### 1. Connector layer — `app/connectors/`

Pulls raw, source-shaped JSON from an external API. One file per source:

- [`base.py`](app/connectors/base.py) — `BaseConnector` abstract base class
- [`factory.py`](app/connectors/factory.py) — `ConnectorFactory.get_connector(name)`, a hardcoded `if/elif` mapping connector name → class
- [`github.py`](app/connectors/github.py), [`basic_auth.py`](app/connectors/basic_auth.py), [`stripe.py`](app/connectors/stripe.py), [`newsapi.py`](app/connectors/newsapi.py), [`openweathermap.py`](app/connectors/openweathermap.py) — one concrete connector each

Connectors know nothing about `CommonData` or Postgres — they only return `List[Dict[str, Any]]` of whatever shape the source API returns.

### 2. Transform layer — `app/transform/transformer.py`

`Transformer.transform(source: str, raw_records: List[Dict]) -> List[CommonData]` maps each source's raw shape into the common schema ([`CommonData`](app/models/schema.py)). There is an explicit `if/elif` branch per `source` string — adding a connector means adding a branch here too, keyed on the same name used in `config.yaml` / the factory.

### 3. Loader layer — `app/loader/postgres.py`

`upsert_data(records: List[CommonData], connector_name: str)` writes to a **dynamically named table per source**: `data_<connector_name>` (see `get_dynamic_table`), created on first use with `checkfirst=True`. Also owns `insert_failed_record()` for the dead-letter table (`failed_records`).

### Supporting modules

- [`app/models/schema.py`](app/models/schema.py) — `CommonData`, the common schema every connector's output is normalized into (`id`, `source`, `title`, `description`, `created_at`, `raw_data`)
- [`app/config.py`](app/config.py) — `Settings` (pydantic-settings). `settings.connectors_config` re-reads `app/config.yaml` on every access and injects secrets from env vars (`GITHUB_TOKEN`, `STRIPE_API_KEY`, etc.) into the per-connector dict before it reaches the connector's `__init__`
- [`app/config.yaml`](app/config.yaml) — non-secret per-connector settings only (base URLs, repo lists, query lists, city lists). Never put API keys/tokens here — they belong in `.env` and get merged in by `config.py`
- [`app/utils/logger.py`](app/utils/logger.py) — `get_logger(name)`, standard stdlib logging to stdout

## `BaseConnector` contract

Every connector subclasses [`BaseConnector`](app/connectors/base.py) and must implement three abstract methods:

```python
class BaseConnector(ABC):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.session = self.authenticate()   # called automatically, do not override __init__ lightly

    @abstractmethod
    def authenticate(self):
        """Build and return a `requests.Session` (or equivalent), applying auth headers/params."""

    @abstractmethod
    def fetch_data(self) -> List[Dict[str, Any]]:
        """Fetch and return all raw records for this source as a flat list of dicts."""

    @abstractmethod
    def handle_pagination(self, *args, **kwargs):
        """Walk pages/cursors for one endpoint/query and return the combined list of raw items."""
```

To add a new connector, all of the following must be done together — they are not independently discoverable:

1. Create `app/connectors/<name>.py` with a class implementing `authenticate`, `fetch_data`, `handle_pagination`.
2. Register it in the `if/elif` chain in [`ConnectorFactory.get_connector`](app/connectors/factory.py).
3. Add a matching entry under `connectors:` in [`app/config.yaml`](app/config.yaml) using the exact same key used in the factory and transformer.
4. Add a branch for that source name in [`Transformer.transform`](app/transform/transformer.py) mapping its raw shape to `CommonData`.
5. If the connector needs a secret, add a field to `Settings` in `app/config.py` and wire it into `connectors_config`, plus document it in `.env.example`.

Existing connectors are useful reference points for different auth styles: `github.py` (Bearer token + Link-header pagination), `basic_auth.py` (HTTP basic auth), `stripe.py` (Bearer token + `starting_after` cursor pagination), `newsapi.py` / `openweathermap.py` (API key in header/query param).

Rate-limited connectors (`github.py`, `basic_auth.py`) wrap their HTTP call in a `_make_request` helper decorated with `tenacity.retry` (exponential backoff, 5 attempts) that raises a local `RateLimitException` on HTTP 429. Follow this pattern for any new connector that can be rate-limited rather than inventing a different retry mechanism.

## Testing conventions

- Framework: `pytest`. Run with `pytest tests/` (or `pytest tests/test_transformer.py -k name` for one test). `pytest.ini` sets `pythonpath = .`, so no `PYTHONPATH` env var juggling is needed.
- **The database is always mocked, never real.** [`tests/test_loader.py`](tests/test_loader.py) patches `app.loader.postgres.SessionLocal` and `Table.create` — no test should open a real Postgres connection. Follow this same `patch("app.loader.postgres.SessionLocal")` pattern for any new loader test.
- [`tests/test_transformer.py`](tests/test_transformer.py) tests `Transformer.transform` directly as a pure function (no mocking needed) — one test per source shape, plus an unknown-source case and a missing-fields case. When adding a transformer branch for a new source, add a corresponding test here.
- Tests assert on both the "happy path" and on `db.rollback()` / `db.commit()` call state for failure paths — when touching loader logic, preserve that a failure always rolls back and never commits (see `test_upsert_data_exception`).

## Do not break

- **Idempotent upserts.** `upsert_data` and `insert_failed_record` in [`app/loader/postgres.py`](app/loader/postgres.py) both use Postgres `INSERT ... ON CONFLICT (id, source) DO UPDATE`, never a plain `INSERT`. The composite primary key is always `(id, source)`. Re-running a sync for the same connector must update existing rows, not duplicate or fail on them. If you touch loader code, keep the `on_conflict_do_update` clause and keep `id`+`source` as the conflict target.
- **Dead-letter behavior on transform failure.** In [`Transformer.transform`](app/transform/transformer.py), each raw record is transformed inside its own `try/except`. On exception, the record is sent to `insert_failed_record(source, raw, error_msg)` (the `failed_records` table) and the loop **continues** — one bad record must never abort the whole batch or crash the sync. Keep this per-record isolation when modifying the transform loop; do not move to a batch-level try/except.
- Connector `fetch_data()` methods similarly wrap each unit of work (per repo, per query, per city) in its own `try/except` and log-and-continue — preserve that pattern rather than letting one failing item kill the whole `fetch_data()` call.
- Table/model naming: data tables are `data_<connector_name>` (sanitized, lowercased); the dead-letter table is the fixed `failed_records`. Don't hardcode a different table name convention.
