import io
import json
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import JSON, Column, DateTime, MetaData, String, Table, create_engine, inspect, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import declarative_base, sessionmaker

from app.config import settings
from app.utils.logger import get_logger

logger = get_logger("loader")

Base = declarative_base()
metadata_obj = MetaData()

# Rows per INSERT statement. None emits a single statement for the whole batch.
# See benchmarks/README.md for the sweep these values were chosen from.
DEFAULT_CHUNK_SIZE = 1000
DEFAULT_DLQ_CHUNK_SIZE = 500

# How upsert_data moves rows into Postgres: "multirow" issues chunked
# INSERT ... ON CONFLICT statements, "copy" streams into a temp staging table
# and merges once. "copy" measured ~5x faster and byte-identical, and is never
# slower even at 50-record batches. See benchmarks/README.md.
DEFAULT_LOAD_STRATEGY = "copy"

DATA_COLUMNS = ("id", "source", "title", "description", "created_at", "raw_data")
UPDATE_COLUMNS = ("title", "description", "created_at", "raw_data")

def _utcnow() -> datetime:
    # Naive UTC, to match the tz-naive DateTime columns below
    return datetime.now(timezone.utc).replace(tzinfo=None)

class FailedRecord(Base):
    __tablename__ = "failed_records"

    id = Column(String, primary_key=True)
    source = Column(String, primary_key=True)
    raw_data = Column(JSON)
    error_reason = Column(String)
    failed_at = Column(DateTime, default=_utcnow)

engine = create_engine(settings.database_url)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def _define_table(table_name: str) -> Table:
    return Table(
        table_name,
        metadata_obj,
        Column("id", String, primary_key=True),
        Column("source", String, primary_key=True),
        Column("title", String),
        Column("description", String, nullable=True),
        Column("created_at", DateTime),
        Column("raw_data", JSON)
    )

def get_dynamic_table(source_name: str, create_if_missing: bool = True) -> Table:
    # Use the source name as the table name
    safe_name = "".join([c if c.isalnum() else "_" for c in source_name]).lower()
    table_name = f"data_{safe_name}"

    if table_name in metadata_obj.tables:
        return metadata_obj.tables[table_name]

    if not create_if_missing:
        if not inspect(engine).has_table(table_name):
            return None
        return _define_table(table_name)

    table = _define_table(table_name)
    table.create(engine, checkfirst=True)
    return table

def init_db():
    logger.info("Initializing database schema...")
    Base.metadata.create_all(bind=engine)

def _failed_record_id(raw_data: dict) -> str:
    return str(raw_data.get("id") or raw_data.get("uuid") or _utcnow().timestamp())


def _failed_record_stmt(rows):
    stmt = insert(FailedRecord).values(rows)
    return stmt.on_conflict_do_update(
        index_elements=["id", "source"],
        set_={
            "raw_data": stmt.excluded.raw_data,
            "error_reason": stmt.excluded.error_reason,
            "failed_at": stmt.excluded.failed_at
        }
    )


def insert_failed_record(source: str, raw_data: dict, error_reason: str):
    """Write a single record to the dead-letter table."""
    insert_failed_records(source, [(raw_data, error_reason)])


def insert_failed_records(source: str, failures, chunk_size: int = DEFAULT_DLQ_CHUNK_SIZE):
    """Write a batch of dead-lettered records in one transaction.

    `failures` is an iterable of `(raw_data, error_reason)` pairs. Batching
    matters because the caller is a per-record failure handler: committing once
    per bad record turns a partly-malformed sync into thousands of round trips.

    Never raises — the dead-letter table is itself the error path, so a failure
    here is logged rather than propagated. If the batch fails, each record is
    retried individually so one poisonous row cannot discard the rest.
    """
    failures = list(failures)
    if not failures:
        return

    # Collapse duplicate keys within the batch: Postgres refuses to let a
    # single INSERT ... ON CONFLICT DO UPDATE touch the same row twice. Records
    # with no id share a timestamp-derived id, so collisions are realistic.
    # Last occurrence wins, matching the old one-statement-per-record order.
    deduped = {}
    for raw_data, error_reason in failures:
        record_id = _failed_record_id(raw_data)
        deduped[(record_id, source)] = {
            "id": record_id,
            "source": source,
            "raw_data": raw_data,
            "error_reason": error_reason,
            "failed_at": _utcnow()
        }
    rows = list(deduped.values())

    size = chunk_size if chunk_size and chunk_size > 0 else len(rows)

    db = SessionLocal()
    try:
        for start in range(0, len(rows), size):
            db.execute(_failed_record_stmt(rows[start:start + size]))
        db.commit()
        logger.info(f"Wrote {len(rows)} dead letter record(s) for source '{source}'.")
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to insert dead letter batch of {len(rows)}: {e}")
        _insert_failed_rows_individually(db, rows)
    finally:
        db.close()


def _insert_failed_rows_individually(db, rows):
    """Fallback for a rejected batch: keep whatever rows can be written."""
    written = 0
    for row in rows:
        try:
            db.execute(_failed_record_stmt([row]))
            db.commit()
            written += 1
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to insert dead letter record {row['id']}: {e}")
    logger.info(f"Recovered {written}/{len(rows)} dead letter record(s) after batch failure.")

def _upsert_multirow(db, table, records, chunk_size: Optional[int]):
    """Chunked INSERT ... ON CONFLICT statements."""
    size = chunk_size if chunk_size and chunk_size > 0 else len(records)

    for start in range(0, len(records), size):
        chunk = records[start:start + size]

        stmt = insert(table).values([{
            "id": r.id,
            "source": r.source,
            "title": r.title,
            "description": r.description,
            "created_at": r.created_at,
            "raw_data": r.raw_data
        } for r in chunk])

        # On conflict on (id, source), do update
        update_dict = {
            "title": stmt.excluded.title,
            "description": stmt.excluded.description,
            "created_at": stmt.excluded.created_at,
            "raw_data": stmt.excluded.raw_data
        }

        stmt = stmt.on_conflict_do_update(
            index_elements=["id", "source"],
            set_=update_dict
        )

        db.execute(stmt)


def _copy_field(value) -> str:
    r"""Encode one value for COPY ... FROM STDIN in the default text format.

    NULL is \N; backslash and the record/field separators must be escaped, or a
    title containing a tab or newline would silently corrupt the stream.
    """
    if value is None:
        return "\\N"
    if isinstance(value, (dict, list)):
        value = json.dumps(value)
    elif isinstance(value, datetime):
        value = value.isoformat()
    else:
        value = str(value)
    return (
        value.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def _copy_buffer(records) -> io.StringIO:
    buf = io.StringIO()
    for r in records:
        buf.write("\t".join((
            _copy_field(r.id),
            _copy_field(r.source),
            _copy_field(r.title),
            _copy_field(r.description),
            _copy_field(r.created_at),
            _copy_field(r.raw_data),
        )))
        buf.write("\n")
    buf.seek(0)
    return buf


def _upsert_copy(db, table, records):
    """Stream rows into a temp staging table, then merge in one statement.

    COPY avoids per-row statement overhead, and the staging table carries no
    index, so the only index maintenance happens once during the merge.
    """
    stage = f"stage_{table.name}"
    columns = ", ".join(DATA_COLUMNS)

    # No indexes on the staging table, and it disappears when we commit.
    # Temp tables do not write WAL for their contents.
    db.execute(text(
        f"CREATE TEMP TABLE {stage} (LIKE {table.name} INCLUDING DEFAULTS) ON COMMIT DROP"
    ))

    cursor = db.connection().connection.cursor()
    try:
        cursor.copy_expert(f"COPY {stage} ({columns}) FROM STDIN", _copy_buffer(records))
    finally:
        cursor.close()

    updates = ", ".join(f"{c} = excluded.{c}" for c in UPDATE_COLUMNS)

    # DISTINCT ON collapses duplicate keys inside the batch — Postgres refuses
    # to let one ON CONFLICT DO UPDATE touch the same row twice. ctid DESC keeps
    # the last-written copy, matching the per-statement path's last-wins order.
    # Ordering by (id, source) also groups index writes instead of scattering.
    db.execute(text(
        f"INSERT INTO {table.name} ({columns}) "
        f"SELECT DISTINCT ON (id, source) {columns} FROM {stage} "
        f"ORDER BY id, source, ctid DESC "
        f"ON CONFLICT (id, source) DO UPDATE SET {updates}"
    ))


def upsert_data(
    records,
    connector_name: str = "default",
    chunk_size: Optional[int] = DEFAULT_CHUNK_SIZE,
    strategy: str = DEFAULT_LOAD_STRATEGY,
):
    """
    Upsert data to PostgreSQL in a dynamically named table.

    Both strategies run in a single transaction with one commit, and both keep
    `(id, source)` as the conflict target, so a re-run updates rather than
    duplicates:

    - "multirow": chunks of `chunk_size` rows per INSERT statement, so no single
      statement grows unbounded. Pass chunk_size=None for one statement.
    - "copy": COPY into a temp staging table, then a single merge.
    """
    if not records:
        return

    db = SessionLocal()
    try:
        table = get_dynamic_table(connector_name)

        if strategy == "copy":
            _upsert_copy(db, table, records)
        elif strategy == "multirow":
            _upsert_multirow(db, table, records, chunk_size)
        else:
            raise ValueError(f"Unknown load strategy: {strategy}")

        db.commit()
        logger.info(
            f"Successfully upserted {len(records)} records into table "
            f"'{table.name}' via {strategy}."
        )
    except Exception as e:
        db.rollback()
        logger.error(f"Error during upsert: {e}")
        raise
    finally:
        db.close()
