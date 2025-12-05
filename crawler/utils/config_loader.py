import os
from typing import Any, Dict, Optional

import yaml

from pydantic_settings import BaseSettings, SettingsConfigDict

from crawler.utils.env_loader import load_environment


DEFAULT_USER_AGENT = "JooyaBot/1.0"


class Config(BaseSettings):
    redis_url: str
    postgres_url: str
    mongo_url: str
    mongo_db: Optional[str] = None
    crawl_delay_default: float = 2.5
    crawler_workers: int = 5
    request_timeout: int = 10
    crawler_user_agent: str = DEFAULT_USER_AGENT
    max_depth: Optional[int] = None
    max_pages: Optional[int] = None

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    log_level: str = "INFO"


def _load_yaml_config() -> Dict[str, Any]:
    config_path = os.path.join(os.path.dirname(__file__), "../config/config.yaml")
    if not os.path.exists(config_path):
        return {}

    with open(config_path, "r") as f:
        return yaml.safe_load(f) or {}


def load_config() -> Config:
    load_environment()
    file_data = _load_yaml_config()
    crawler_settings: Dict[str, Any] = file_data.get("crawler") or {}

    # خواندن از environment
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    postgres_url = os.getenv("POSTGRES_URL", "postgresql://localhost:5432/db")
    mongo_url = os.getenv("MONGO_URI") or os.getenv("MONGO_URL") or "mongodb://localhost:27017"
    mongo_db = os.getenv("MONGO_DB") or crawler_settings.get("mongo_db")

    # User-agent precedence: env -> config file -> default
    user_agent = os.getenv("CRAWLER_USER_AGENT")
    if not user_agent:
        user_agent = crawler_settings.get("user_agent")
    if not user_agent:
        user_agent = DEFAULT_USER_AGENT

    max_depth = crawler_settings.get("max_depth")
    max_pages = crawler_settings.get("max_pages")
    crawl_delay_default = crawler_settings.get("crawl_delay_default") or 2.5

    return Config(
        redis_url=redis_url,
        postgres_url=postgres_url,
        mongo_url=mongo_url,
        mongo_db=mongo_db,
        crawler_user_agent=user_agent,
        max_depth=max_depth,
        max_pages=max_pages,
        crawl_delay_default=crawl_delay_default,
        **{k: v for k, v in file_data.items() if k != "crawler"},
    )


def get_crawler_user_agent() -> str:
    """Return the configured crawler user-agent string."""
    config = load_config()
    return config.crawler_user_agent
