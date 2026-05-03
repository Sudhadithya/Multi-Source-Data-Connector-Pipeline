import requests
from typing import List, Dict, Any
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

from app.connectors.base import BaseConnector
from app.utils.logger import get_logger

logger = get_logger("github_connector")

class RateLimitException(Exception):
    pass

class GitHubConnector(BaseConnector):
    def authenticate(self):
        session = requests.Session()
        token = self.config.get("token")
        if token and token != "your_github_token_here":
            session.headers.update({"Authorization": f"Bearer {token}"})
        session.headers.update({"Accept": "application/vnd.github.v3+json"})
        return session

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(RateLimitException)
    )
    def _make_request(self, url: str) -> requests.Response:
        response = self.session.get(url)
        if response.status_code == 429 or (
            response.status_code == 403 and "rate limit" in response.headers.get("x-ratelimit-remaining", "")
        ):
            logger.warning("Rate limit hit, raising RateLimitException to trigger retry.")
            raise RateLimitException("Rate limit exceeded")
        response.raise_for_status()
        return response

    def fetch_data(self) -> List[Dict[str, Any]]:
        base_url = self.config.get("base_url", "https://api.github.com")
        repos = self.config.get("repos", [])
        all_data = []

        for repo in repos:
            url = f"{base_url}/repos/{repo}"
            try:
                # Fetch repo info
                logger.info(f"Fetching repo info for {repo}")
                response = self._make_request(url)
                all_data.append(response.json())
                
                # Fetch issues for the repo using pagination
                # For demonstration, limit to a small number of pages or items
                issues_url = f"{url}/issues"
                issues = self.handle_pagination(issues_url)
                all_data.extend(issues)
            except Exception as e:
                logger.error(f"Error fetching data for repo {repo}: {e}")

        return all_data

    def handle_pagination(self, url: str) -> List[Dict[str, Any]]:
        data = []
        page = 1
        # Fetch up to 2 pages for testing to avoid huge sync times
        while page <= 2:
            paginated_url = f"{url}?page={page}&per_page=30"
            logger.info(f"Fetching {paginated_url}")
            response = self._make_request(paginated_url)
            page_data = response.json()
            if not page_data:
                break
            data.extend(page_data)
            
            # Check link header for next page
            if "next" not in response.links:
                break
            page += 1
        return data
