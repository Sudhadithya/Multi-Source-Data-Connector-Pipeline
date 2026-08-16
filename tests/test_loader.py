from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from app.loader.postgres import (
    _copy_buffer,
    _copy_field,
    get_dynamic_table,
    insert_failed_record,
    insert_failed_records,
    upsert_data,
)
from app.models.schema import CommonData


def _record(record_id: str) -> CommonData:
    return CommonData(
        id=record_id,
        source="github",
        title="test",
        created_at=datetime.now(timezone.utc),
        raw_data={"test": "data"}
    )


@pytest.fixture
def mock_db_session():
    with patch("app.loader.postgres.SessionLocal") as mock_session:
        with patch("app.loader.postgres.Table.create"):
            yield mock_session

def test_upsert_data_success(mock_db_session):
    mock_session_instance = mock_db_session.return_value

    records = [
        CommonData(
            id="1",
            source="github",
            title="test",
            created_at=datetime.now(timezone.utc),
            raw_data={"test": "data"}
        )
    ]

    upsert_data(records)  # default strategy

    assert mock_session_instance.execute.called
    assert mock_session_instance.commit.called
    assert not mock_session_instance.rollback.called

def test_upsert_data_empty(mock_db_session):
    mock_session_instance = mock_db_session.return_value

    upsert_data([])

    assert not mock_session_instance.execute.called

def test_upsert_data_exception(mock_db_session):
    mock_session_instance = mock_db_session.return_value
    mock_session_instance.execute.side_effect = Exception("DB Error")

    records = [
        CommonData(
            id="1",
            source="github",
            title="test",
            created_at=datetime.now(timezone.utc),
            raw_data={}
        )
    ]

    with pytest.raises(Exception, match="DB Error"):
        upsert_data(records)

    assert mock_session_instance.rollback.called
    assert not mock_session_instance.commit.called

def test_insert_failed_record_success(mock_db_session):
    mock_session_instance = mock_db_session.return_value

    insert_failed_record("github", {"id": "123"}, "Test error")

    assert mock_session_instance.execute.called
    assert mock_session_instance.commit.called

def test_upsert_data_chunks_but_commits_once(mock_db_session):
    mock_session_instance = mock_db_session.return_value

    records = [_record(str(i)) for i in range(5)]

    upsert_data(records, "github", chunk_size=2, strategy="multirow")

    # 5 records at 2 per statement -> 3 statements, still one transaction.
    assert mock_session_instance.execute.call_count == 3
    assert mock_session_instance.commit.call_count == 1
    assert not mock_session_instance.rollback.called

def test_upsert_data_single_statement_when_chunk_size_none(mock_db_session):
    mock_session_instance = mock_db_session.return_value

    upsert_data([_record(str(i)) for i in range(5)], "github", chunk_size=None, strategy="multirow")

    assert mock_session_instance.execute.call_count == 1
    assert mock_session_instance.commit.call_count == 1

def test_copy_field_escapes_copy_control_characters():
    # Tab and newline are COPY's field and record separators, backslash is its
    # escape character: unescaped, any of them corrupts the whole stream.
    assert _copy_field(None) == "\\N"
    assert _copy_field("plain") == "plain"
    assert _copy_field("a\tb") == "a\\tb"
    assert _copy_field("a\nb") == "a\\nb"
    assert _copy_field("a\r\nb") == "a\\r\\nb"
    assert _copy_field("a\\b") == "a\\\\b"
    # A literal backslash-N must not be mistaken for the NULL marker.
    assert _copy_field("\\N") == "\\\\N"
    assert _copy_field("") == ""

def test_copy_field_serializes_structured_values():
    assert _copy_field({"a": 1}) == '{"a": 1}'
    assert _copy_field([1, "two"]) == '[1, "two"]'
    assert _copy_field(datetime(2024, 1, 15, 10, 30)) == "2024-01-15T10:30:00"

def test_copy_buffer_emits_one_tab_delimited_line_per_record():
    records = [_record("1"), _record("2")]

    lines = _copy_buffer(records).getvalue().splitlines()

    assert len(lines) == 2
    assert all(len(line.split("\t")) == 6 for line in lines)
    assert lines[0].startswith("1\tgithub\t")

def test_copy_buffer_writes_null_marker_for_missing_description():
    record = _record("1")
    record.description = None

    line = _copy_buffer([record]).getvalue().rstrip("\n")

    assert line.split("\t")[3] == "\\N"

def test_upsert_data_copy_strategy_streams_and_merges(mock_db_session):
    mock_session_instance = mock_db_session.return_value

    upsert_data([_record("1")], "github", strategy="copy")

    cursor = mock_session_instance.connection.return_value.connection.cursor.return_value
    assert cursor.copy_expert.called
    copy_sql = cursor.copy_expert.call_args[0][0]
    assert copy_sql.startswith("COPY stage_data_github")

    # CREATE TEMP TABLE, then the merge back into the real table.
    statements = [str(c[0][0]) for c in mock_session_instance.execute.call_args_list]
    assert any("CREATE TEMP TABLE" in s for s in statements)
    assert any("ON CONFLICT (id, source) DO UPDATE" in s for s in statements)
    assert any("DISTINCT ON (id, source)" in s for s in statements)
    assert mock_session_instance.commit.call_count == 1

def test_upsert_data_rejects_unknown_strategy(mock_db_session):
    mock_session_instance = mock_db_session.return_value

    with pytest.raises(ValueError, match="Unknown load strategy"):
        upsert_data([_record("1")], "github", strategy="magic")

    assert mock_session_instance.rollback.called
    assert not mock_session_instance.commit.called

def test_insert_failed_records_batches_into_one_transaction(mock_db_session):
    mock_session_instance = mock_db_session.return_value

    failures = [({"id": str(i)}, "boom") for i in range(4)]

    insert_failed_records("github", failures, chunk_size=100)

    assert mock_session_instance.execute.call_count == 1
    assert mock_session_instance.commit.call_count == 1

def test_insert_failed_records_empty_does_nothing(mock_db_session):
    mock_session_instance = mock_db_session.return_value

    insert_failed_records("github", [])

    assert not mock_session_instance.execute.called

def test_insert_failed_records_dedupes_conflicting_keys(mock_db_session):
    mock_session_instance = mock_db_session.return_value

    # Postgres rejects an ON CONFLICT DO UPDATE touching the same row twice,
    # so same-key failures must collapse before the statement is built.
    failures = [({"id": "dup"}, "first"), ({"id": "dup"}, "second")]

    insert_failed_records("github", failures, chunk_size=100)

    assert mock_session_instance.execute.call_count == 1
    params = mock_session_instance.execute.call_args[0][0].compile().params
    assert "second" in params.values()
    assert "first" not in params.values()

def test_insert_failed_records_falls_back_to_individual_writes(mock_db_session):
    mock_session_instance = mock_db_session.return_value
    # Batch statement fails; the two per-record retries succeed.
    mock_session_instance.execute.side_effect = [Exception("batch rejected"), None, None]

    insert_failed_records("github", [({"id": "1"}, "a"), ({"id": "2"}, "b")], chunk_size=100)

    assert mock_session_instance.execute.call_count == 3
    assert mock_session_instance.rollback.call_count == 1
    assert mock_session_instance.commit.call_count == 2

def test_insert_failed_records_never_raises(mock_db_session):
    mock_session_instance = mock_db_session.return_value
    mock_session_instance.execute.side_effect = Exception("DB down")

    # The dead-letter table is itself the error path: it must not raise.
    insert_failed_records("github", [({"id": "1"}, "a")])

    assert mock_session_instance.rollback.called

def test_get_dynamic_table_returns_none_when_missing_and_not_creating():
    with patch("app.loader.postgres.inspect") as mock_inspect, \
         patch("app.loader.postgres.Table.create") as mock_create:
        mock_inspect.return_value.has_table.return_value = False

        table = get_dynamic_table("nonexistent_source_xyz", create_if_missing=False)

        assert table is None
        assert not mock_create.called

def test_get_dynamic_table_returns_existing_table_without_creating():
    with patch("app.loader.postgres.inspect") as mock_inspect, \
         patch("app.loader.postgres.Table.create") as mock_create:
        mock_inspect.return_value.has_table.return_value = True

        table = get_dynamic_table("existing_source_xyz", create_if_missing=False)

        assert table is not None
        assert table.name == "data_existing_source_xyz"
        assert not mock_create.called

def test_get_dynamic_table_creates_when_create_if_missing_true():
    with patch("app.loader.postgres.Table.create") as mock_create:
        table = get_dynamic_table("brand_new_source_xyz", create_if_missing=True)

        assert table.name == "data_brand_new_source_xyz"
        assert mock_create.called
