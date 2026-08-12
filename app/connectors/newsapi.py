import requests
from typing import List, Dict, Any

from app.connectors.base import BaseConnector
from app.utils.logger import get_logger

logger = get_logger("newsapi_connector")

class NewsAPIConnector(BaseConnector):
    def authenticate(self):
        session = requests.Session()
        api_key = self.config.get("api_key")
        if api_key:
            session.headers.update({"X-Api-Key": api_key})
        return session

    def fetch_data(self) -> List[Dict[str, Any]]:
        base_url = self.config.get("base_url", "https://newsapi.org/v2")
        queries = self.config.get("queries", [])
        all_data = []

        for query in queries:
            url = f"{base_url}/everything?q={query}"
            try:
                logger.info(f"Fetching NewsAPI data for query '{query}'")
                data = self.handle_pagination(url)
                for item in data:
                    item["_newsapi_query"] = query # Custom field to help transformer
                all_data.extend(data)
            except Exception as e:
                logger.error(f"Error fetching NewsAPI for query '{query}': {e}")

        return all_data

    def handle_pagination(self, url: str) -> List[Dict[str, Any]]:
        data = []
        
        # NewsAPI free tier limits to 100 results total, page size is max 100
        # So we just fetch page 1
        paginated_url = f"{url}&pageSize=100&page=1"
        response = self.session.get(paginated_url)
        response.raise_for_status()
        
        resp_json = response.json()
        articles = resp_json.get("articles", [])
        if articles:
            data.extend(articles)
            
        return data
