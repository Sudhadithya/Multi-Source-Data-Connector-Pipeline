import pytest
from app.transform.transformer import Transformer

def test_transform_github_valid():
    raw_data = [
        {
            "id": 123,
            "name": "test-repo",
            "description": "A test repo",
            "created_at": "2023-01-01T00:00:00Z"
        }
    ]
    transformer = Transformer()
    transformed = transformer.transform("github", raw_data)
    
    assert len(transformed) == 1
    assert transformed[0].id == "123"
    assert transformed[0].title == "test-repo"
    assert transformed[0].description == "A test repo"
    assert transformed[0].source == "github"

def test_transform_basic_auth_valid():
    raw_data = [
        {
            "uuid": "abc-123",
            "name": "Auth Resource",
            "details": "Resource details"
        }
    ]
    transformer = Transformer()
    transformed = transformer.transform("basic_auth_api", raw_data)
    
    assert len(transformed) == 1
    assert transformed[0].id == "abc-123"
    assert transformed[0].title == "Auth Resource"
    assert transformed[0].description == "Resource details"
    assert transformed[0].source == "basic_auth_api"

def test_transform_unknown_source(caplog):
    raw_data = [{"id": 1}]
    transformer = Transformer()
    transformed = transformer.transform("unknown_source", raw_data)
    
    assert len(transformed) == 0
    assert "Unknown source: unknown_source" in caplog.text

def test_transform_handles_missing_fields():
    raw_data = [{}] # completely empty
    transformer = Transformer()
    transformed = transformer.transform("github", raw_data)
    
    assert len(transformed) == 1
    assert transformed[0].title == "Untitled"
    assert transformed[0].description is None
