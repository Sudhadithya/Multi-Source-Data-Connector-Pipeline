import pytest
from unittest.mock import MagicMock, patch
from app.loader.postgres import upsert_data, insert_failed_record
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
