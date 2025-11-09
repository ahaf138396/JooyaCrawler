import os
import yaml
from pydantic import BaseModel

class Config(BaseModel):
    redis_url: str
    postgres_url: str
    mongo_url: str
    crawler_workers: int = 4
    crawl_depth_limit: int = 3
    crawl_delay_default: float = 2.0
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
