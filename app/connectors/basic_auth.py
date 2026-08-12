from typing import Any, Dict, List

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.connectors.base import BaseConnector
from app.utils.logger import get_logger

logger = get_logger("basic_auth_connector")

class RateLimitException(Exception):
    pass

class BasicAuthConnector(BaseConnector):
    def authenticate(self):
        session = requests.Session()
        username = self.config.get("username")
        password = self.config.get("password")
        if username and password:
            session.auth = (username, password)
        return session

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(RateLimitException)
    )
    def _make_request(self, url: str) -> requests.Response:
        response = self.session.get(url)
        if response.status_code == 429:
            logger.warning("Rate limit hit, raising RateLimitException to trigger retry.")
            raise RateLimitException("Rate limit exceeded")
        response.raise_for_status()
        return response

    def fetch_data(self) -> List[Dict[str, Any]]:
        base_url = self.config.get("base_url")
        all_data = []

        try:
            logger.info(f"Fetching data from basic auth api {base_url}")
            response = self._make_request(base_url)
            # httpbin basic-auth returns {"authenticated": true, "user": "user"}
            data = response.json()
            # adding some id and name to fit the schema
            data["id"] = "basic_auth_1"
            data["name"] = "Basic Auth Sample Data"
            all_data.append(data)
        except Exception as e:
            logger.error(f"Error fetching basic auth data: {e}")

        return all_data

    def handle_pagination(self, url: str) -> List[Dict[str, Any]]:
        # Mock pagination logic for a simple basic auth endpoint
        return []
