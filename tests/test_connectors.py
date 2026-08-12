from unittest.mock import MagicMock

from app.connectors.newsapi import NewsAPIConnector
from app.connectors.openweathermap import OpenWeatherMapConnector
from app.connectors.stripe import StripeConnector


def make_connector(config=None):
    return StripeConnector(config or {"api_key": "sk_test_123"})


def make_newsapi_connector(config=None):
    return NewsAPIConnector(config or {"api_key": "news_test_123"})


def make_owm_connector(config=None):
    return OpenWeatherMapConnector(config or {"api_key": "owm_test_123"})


def test_stripe_authenticate_sets_bearer_header():
    connector = make_connector()
    assert connector.session.headers["Authorization"] == "Bearer sk_test_123"


def test_stripe_authenticate_without_api_key_skips_header():
    connector = StripeConnector({})
    assert "Authorization" not in connector.session.headers


def test_stripe_handle_pagination_follows_cursor():
    connector = make_connector()

    page1 = MagicMock()
    page1.json.return_value = {
        "data": [{"id": "cus_1"}, {"id": "cus_2"}],
        "has_more": True,
    }
    page2 = MagicMock()
    page2.json.return_value = {
        "data": [{"id": "cus_3"}],
        "has_more": True,  # would continue, but page cap should stop us at 2
    }
    connector.session = MagicMock()
    connector.session.get.side_effect = [page1, page2]

    result = connector.handle_pagination("https://api.stripe.com/v1/customers")

    assert [item["id"] for item in result] == ["cus_1", "cus_2", "cus_3"]
    assert connector.session.get.call_count == 2
    # second call must carry the cursor from the last item of page 1
    second_call_url = connector.session.get.call_args_list[1].args[0]
    assert "starting_after=cus_2" in second_call_url


def test_stripe_handle_pagination_stops_when_no_more_data():
    connector = make_connector()

    page1 = MagicMock()
    page1.json.return_value = {"data": [{"id": "cus_1"}], "has_more": False}
    connector.session = MagicMock()
    connector.session.get.side_effect = [page1]

    result = connector.handle_pagination("https://api.stripe.com/v1/customers")

    assert len(result) == 1
    assert connector.session.get.call_count == 1


def test_stripe_fetch_data_tags_records_with_endpoint():
    connector = make_connector({"api_key": "sk_test", "endpoints": ["customers"]})
    connector.handle_pagination = MagicMock(return_value=[{"id": "cus_1"}, {"id": "cus_2"}])

    result = connector.fetch_data()

    assert len(result) == 2
    assert all(item["_stripe_type"] == "customers" for item in result)


def test_stripe_fetch_data_continues_after_one_endpoint_fails():
    connector = make_connector(
        {"api_key": "sk_test", "endpoints": ["customers", "charges"]}
    )

    def side_effect(url):
        if "customers" in url:
            raise Exception("boom")
        return [{"id": "ch_1"}]

    connector.handle_pagination = MagicMock(side_effect=side_effect)

    result = connector.fetch_data()

    assert len(result) == 1
    assert result[0]["id"] == "ch_1"
    assert result[0]["_stripe_type"] == "charges"


def test_newsapi_authenticate_sets_api_key_header():
    connector = make_newsapi_connector()
    assert connector.session.headers["X-Api-Key"] == "news_test_123"


def test_newsapi_authenticate_without_api_key_skips_header():
    connector = NewsAPIConnector({})
    assert "X-Api-Key" not in connector.session.headers


def test_newsapi_handle_pagination_returns_articles():
    connector = make_newsapi_connector()
    response = MagicMock()
    response.json.return_value = {"articles": [{"title": "A"}, {"title": "B"}]}
    connector.session = MagicMock()
    connector.session.get.return_value = response

    result = connector.handle_pagination("https://newsapi.org/v2/everything?q=python")

    assert len(result) == 2
    assert connector.session.get.call_count == 1


def test_newsapi_handle_pagination_returns_empty_when_no_articles():
    connector = make_newsapi_connector()
    response = MagicMock()
    response.json.return_value = {"articles": []}
    connector.session = MagicMock()
    connector.session.get.return_value = response

    result = connector.handle_pagination("https://newsapi.org/v2/everything?q=python")

    assert result == []


def test_newsapi_fetch_data_tags_records_with_query():
    connector = make_newsapi_connector({"api_key": "news_test", "queries": ["python"]})
    connector.handle_pagination = MagicMock(return_value=[{"title": "A"}, {"title": "B"}])

    result = connector.fetch_data()

    assert len(result) == 2
    assert all(item["_newsapi_query"] == "python" for item in result)


def test_newsapi_fetch_data_continues_after_one_query_fails():
    connector = make_newsapi_connector(
        {"api_key": "news_test", "queries": ["python", "technology"]}
    )

    def side_effect(url):
        if "q=python" in url:
            raise Exception("boom")
        return [{"title": "Tech News"}]

    connector.handle_pagination = MagicMock(side_effect=side_effect)

    result = connector.fetch_data()

    assert len(result) == 1
    assert result[0]["title"] == "Tech News"
    assert result[0]["_newsapi_query"] == "technology"


def test_owm_authenticate_returns_session_without_headers():
    connector = make_owm_connector()
    assert "Authorization" not in connector.session.headers
    assert "X-Api-Key" not in connector.session.headers


def test_owm_fetch_data_without_api_key_returns_empty():
    connector = OpenWeatherMapConnector({"cities": ["London"]})
    result = connector.fetch_data()
    assert result == []


def test_owm_fetch_data_tags_records_with_city():
    connector = make_owm_connector({"api_key": "owm_test", "cities": ["London"]})
    response = MagicMock()
    response.json.return_value = {"name": "London", "weather": [{"description": "clear sky"}]}
    connector.session = MagicMock()
    connector.session.get.return_value = response

    result = connector.fetch_data()

    assert len(result) == 1
    assert result[0]["_city_query"] == "London"


def test_owm_fetch_data_continues_after_one_city_fails():
    connector = make_owm_connector({"api_key": "owm_test", "cities": ["Nowhere", "London"]})

    def side_effect(url):
        if "q=Nowhere" in url:
            raise Exception("boom")
        response = MagicMock()
        response.json.return_value = {"name": "London"}
        return response

    connector.session = MagicMock()
    connector.session.get.side_effect = side_effect

    result = connector.fetch_data()

    assert len(result) == 1
    assert result[0]["_city_query"] == "London"
