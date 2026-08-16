from unittest.mock import patch

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

def test_transform_stripe_valid():
    raw_data = [
        {
            "id": "cus_123",
            "email": "test@example.com",
            "amount": 4200,
            "created": 1672531200,
            "_stripe_type": "customers"
        }
    ]
    transformer = Transformer()
    transformed = transformer.transform("stripe", raw_data)

    assert len(transformed) == 1
    assert transformed[0].id == "cus_123"
    assert transformed[0].title == "test@example.com"
    assert transformed[0].description == "4200"
    assert transformed[0].source == "stripe"
    assert transformed[0].created_at.year == 2023

def test_transform_stripe_missing_fields():
    raw_data = [{}]
    transformer = Transformer()
    transformed = transformer.transform("stripe", raw_data)

    assert len(transformed) == 1
    assert transformed[0].title == "Stripe Object"
    assert transformed[0].description == ""

def test_transform_newsapi_valid():
    raw_data = [
        {
            "url": "https://example.com/article",
            "title": "Big Tech News",
            "description": "Something happened",
            "publishedAt": "2023-05-01T12:00:00Z",
            "_newsapi_query": "technology"
        }
    ]
    transformer = Transformer()
    transformed = transformer.transform("newsapi", raw_data)

    assert len(transformed) == 1
    assert transformed[0].id == "https://example.com/article"
    assert transformed[0].title == "Big Tech News"
    assert transformed[0].description == "Something happened"
    assert transformed[0].source == "newsapi"

def test_transform_newsapi_missing_fields():
    raw_data = [{}]
    transformer = Transformer()
    transformed = transformer.transform("newsapi", raw_data)

    assert len(transformed) == 1
    assert transformed[0].title == "News Article"
    assert transformed[0].description == ""

def test_transform_openweathermap_valid():
    raw_data = [
        {
            "id": 2643743,
            "name": "London",
            "weather": [{"description": "clear sky"}],
            "main": {"temp": 15.5},
            "dt": 1672531200,
            "_city_query": "London"
        }
    ]
    transformer = Transformer()
    transformed = transformer.transform("openweathermap", raw_data)

    assert len(transformed) == 1
    assert transformed[0].id == "2643743"
    assert transformed[0].title == "Weather in London"
    assert transformed[0].description == "clear sky, 15.5°C"
    assert transformed[0].source == "openweathermap"

def test_transform_openweathermap_missing_fields():
    raw_data = [{}]
    transformer = Transformer()
    transformed = transformer.transform("openweathermap", raw_data)

    assert len(transformed) == 1
    assert transformed[0].title == "Weather in Unknown City"
    assert transformed[0].description == ", °C"

def test_transform_unknown_source(caplog):
    raw_data = [{"id": 1}]
    transformer = Transformer()
    transformed = transformer.transform("unknown_source", raw_data)

    assert len(transformed) == 0
    assert "Unknown source: unknown_source" in caplog.text

def test_transform_dead_letters_bad_records_and_continues():
    raw_data = [
        {"id": 1, "name": "good-one", "created_at": "2023-01-01T00:00:00Z"},
        {"id": 2, "name": "bad-one", "created_at": "not-a-timestamp"},
        {"id": 3, "name": "good-two", "created_at": "2023-01-02T00:00:00Z"},
        {"id": 4, "name": "bad-two", "created_at": "also-not-a-timestamp"},
    ]

    with patch("app.loader.postgres.insert_failed_records") as mock_dlq:
        transformed = Transformer().transform("github", raw_data)

    # One bad record must never abort the batch.
    assert [t.id for t in transformed] == ["1", "3"]

    # Both failures are written, and in a single batched call.
    assert mock_dlq.call_count == 1
    source, failures = mock_dlq.call_args[0]
    assert source == "github"
    assert [raw["id"] for raw, _ in failures] == [2, 4]
    assert all("Error transforming record" in reason for _, reason in failures)

def test_transform_does_not_touch_dlq_when_all_records_are_valid():
    raw_data = [{"id": 1, "name": "good", "created_at": "2023-01-01T00:00:00Z"}]

    with patch("app.loader.postgres.insert_failed_records") as mock_dlq:
        transformed = Transformer().transform("github", raw_data)

    assert len(transformed) == 1
    assert not mock_dlq.called

def test_transform_handles_missing_fields():
    raw_data = [{}] # completely empty
    transformer = Transformer()
    transformed = transformer.transform("github", raw_data)

    assert len(transformed) == 1
    assert transformed[0].title == "Untitled"
    assert transformed[0].description is None
