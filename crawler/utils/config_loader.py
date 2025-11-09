import os
import yaml
from pydantic import BaseModel
from httpx import Timeout

from pydantic_settings import BaseSettings

from pydantic_settings import BaseSettings

class Config(BaseSettings):
    redis_url: str
    postgres_url: str
    mongo_url: str
    crawl_delay_default: float = 2.5
    crawler_workers: int = 5
    request_timeout: int = 10

    class Config:
        env_file = "./env/crawler.env"
        extra = "ignore"

    log_level: str = "INFO"

def load_config() -> Config:
    # خواندن از environment
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    postgres_url = os.getenv("POSTGRES_URL", "postgresql://user:pass@localhost:5432/db")
    mongo_url = os.getenv("MONGO_URL", "mongodb://localhost:27017")

    # اگر فایل config.yaml موجود بود، ازش override کن
    config_path = os.path.join(os.path.dirname(__file__), "../config/config.yaml")
    file_data = {}
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            file_data = yaml.safe_load(f) or {}

    return Config(
        redis_url=redis_url,
        postgres_url=postgres_url,
        mongo_url=mongo_url,
        **file_data
    )
