from abc import ABC, abstractmethod
from typing import Any, Dict, List


class BaseConnector(ABC):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.session = self.authenticate()

    @abstractmethod
    def authenticate(self):
        """Authenticate and return a session object."""
        pass

    @abstractmethod
    def fetch_data(self) -> List[Dict[str, Any]]:
        """Fetch data from the source, incorporating pagination."""
        pass

    @abstractmethod
    def handle_pagination(self, *args, **kwargs):
        """Handle pagination logic."""
        pass
