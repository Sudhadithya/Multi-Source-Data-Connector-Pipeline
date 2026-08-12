from app.connectors.github import GitHubConnector
from app.connectors.basic_auth import BasicAuthConnector
from app.connectors.stripe import StripeConnector
from app.connectors.newsapi import NewsAPIConnector
from app.connectors.openweathermap import OpenWeatherMapConnector
from app.config import settings

class ConnectorFactory:
    @staticmethod
    def get_connector(name: str):
        config = settings.connectors_config.get(name)
        if not config:
            raise ValueError(f"No configuration found for connector: {name}")

        if name == "github":
            return GitHubConnector(config)
        elif name == "basic_auth_api":
            return BasicAuthConnector(config)
        elif name == "stripe":
            return StripeConnector(config)
        elif name == "newsapi":
            return NewsAPIConnector(config)
        elif name == "openweathermap":
            return OpenWeatherMapConnector(config)
        else:
            raise ValueError(f"Unknown connector type: {name}")
