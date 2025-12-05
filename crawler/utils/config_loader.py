import os

import yaml

from pydantic_settings import BaseSettings

from crawler.utils.env_loader import load_environment


DEFAULT_USER_AGENT = "JooyaCrawler/0.1 (+https://example.com)"

class Config(BaseSettings):
    redis_url: str
    postgres_url: str
    mongo_url: str
    crawl_delay_default: float = 2.5
    crawler_workers: int = 5
    request_timeout: int = 10
    crawler_user_agent: str = DEFAULT_USER_AGENT

    class Config:
        env_file = ".env"
        extra = "ignore"

    log_level: str = "INFO"

def load_config() -> Config:
    load_environment()
    # خواندن از environment
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    postgres_url = os.getenv("POSTGRES_URL", "postgresql://localhost:5432/db")
    mongo_url = os.getenv("MONGO_URL", "mongodb://localhost:27017")

    crawler_settings = {}

    # اگر فایل config.yaml موجود بود، ازش override کن
    config_path = os.path.join(os.path.dirname(__file__), "../config/config.yaml")
    file_data = {}
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            file_data = yaml.safe_load(f) or {}

            crawler_settings = file_data.get("crawler") or {}

    user_agent = os.getenv("CRAWLER_USER_AGENT")
    if not user_agent and isinstance(crawler_settings, dict):
        user_agent = crawler_settings.get("user_agent")

    if not user_agent:
        user_agent = DEFAULT_USER_AGENT

    return Config(
        redis_url=redis_url,
        postgres_url=postgres_url,
        mongo_url=mongo_url,
        crawler_user_agent=user_agent,
        **file_data
    )


def get_crawler_user_agent() -> str:
    """Return the configured crawler user-agent string."""
    config = load_config()
    return config.crawler_user_agent
