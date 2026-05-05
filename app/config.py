import os
import yaml
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    database_url: str = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5433/pipeline_db")
    config_file: str = "app/config.yaml"
    
    github_token: str = ""
    basic_auth_username: str = ""
    basic_auth_password: str = ""

    model_config = {
        "env_file": ".env",
        "extra": "ignore"
    }

    @property
    def connectors_config(self):
        with open(self.config_file, "r") as f:
            config = yaml.safe_load(f).get("connectors", {})
            # Inject secrets from env vars
            if "github" in config and self.github_token:
                config["github"]["token"] = self.github_token
            if "basic_auth_api" in config:
                if self.basic_auth_username:
                    config["basic_auth_api"]["username"] = self.basic_auth_username
                if self.basic_auth_password:
                    config["basic_auth_api"]["password"] = self.basic_auth_password
            return config

settings = Settings()
