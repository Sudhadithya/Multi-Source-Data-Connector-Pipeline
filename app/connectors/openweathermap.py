import requests
from typing import List, Dict, Any

from app.connectors.base import BaseConnector
from app.utils.logger import get_logger

logger = get_logger("openweathermap_connector")

class OpenWeatherMapConnector(BaseConnector):
    def authenticate(self):
        # OpenWeatherMap uses the key in query params, so session just needs standard headers
        session = requests.Session()
        return session

    def fetch_data(self) -> List[Dict[str, Any]]:
        base_url = self.config.get("base_url", "https://api.openweathermap.org/data/2.5")
        cities = self.config.get("cities", [])
        api_key = self.config.get("api_key")
        all_data = []

        if not api_key:
            logger.error("No API key provided for OpenWeatherMap")
            return all_data

        for city in cities:
            # We'll fetch current weather for the city
            url = f"{base_url}/weather?q={city}&appid={api_key}&units=metric"
            try:
                logger.info(f"Fetching OpenWeatherMap data for '{city}'")
                response = self.session.get(url)
                response.raise_for_status()
                data = response.json()
                # OpenWeatherMap returns a single object for current weather
                if data:
                    data["_city_query"] = city # Custom field
                    all_data.append(data)
            except Exception as e:
                logger.error(f"Error fetching OpenWeatherMap for '{city}': {e}")

        return all_data

    def handle_pagination(self, url: str) -> List[Dict[str, Any]]:
        # Not applicable for simple weather endpoint
        pass
