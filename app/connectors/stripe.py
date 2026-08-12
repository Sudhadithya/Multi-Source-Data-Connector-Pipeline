import requests
from typing import List, Dict, Any

from app.connectors.base import BaseConnector
from app.utils.logger import get_logger

logger = get_logger("stripe_connector")

class StripeConnector(BaseConnector):
    def authenticate(self):
        session = requests.Session()
        api_key = self.config.get("api_key")
        if api_key:
            session.headers.update({"Authorization": f"Bearer {api_key}"})
        return session

    def fetch_data(self) -> List[Dict[str, Any]]:
        base_url = self.config.get("base_url", "https://api.stripe.com/v1")
        endpoints = self.config.get("endpoints", [])
        all_data = []

        for endpoint in endpoints:
            url = f"{base_url}/{endpoint}"
            try:
                logger.info(f"Fetching Stripe data from {url}")
                data = self.handle_pagination(url)
                for item in data:
                    item["_stripe_type"] = endpoint # Custom field to help transformer
                all_data.extend(data)
            except Exception as e:
                logger.error(f"Error fetching Stripe {endpoint}: {e}")

        return all_data

    def handle_pagination(self, url: str) -> List[Dict[str, Any]]:
        data = []
        has_more = True
        starting_after = None
        
        # Limit to 2 pages for demo purposes
        page = 0
        while has_more and page < 2:
            paginated_url = f"{url}?limit=100"
            if starting_after:
                paginated_url += f"&starting_after={starting_after}"
                
            response = self.session.get(paginated_url)
            response.raise_for_status()
            
            resp_json = response.json()
            page_data = resp_json.get("data", [])
            if not page_data:
                break
                
            data.extend(page_data)
            has_more = resp_json.get("has_more", False)
            if has_more:
                starting_after = page_data[-1].get("id")
            page += 1
            
        return data
