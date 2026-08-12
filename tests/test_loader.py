import pytest
from unittest.mock import MagicMock, patch
from app.loader.postgres import upsert_data, insert_failed_record, get_dynamic_table
from app.models.schema import CommonData
from datetime import datetime, timezone

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
    
    upsert_data(records)
    
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
