import os
from tortoise import Tortoise
from loguru import logger

DB_USER = os.getenv("POSTGRES_USER", "jooya")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "SuperSecurePass123")
DB_NAME = os.getenv("POSTGRES_DB", "jooyacrawlerdb")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

DB_URL = f"postgres://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

async def init_postgres():
    logger.info("Initializing PostgreSQL and ORM models...")
    await Tortoise.init(
        db_url=DB_URL,
        modules={"models": ["crawler.storage.models.queue_model"]},
    )
    await Tortoise.generate_schemas(safe=True)
    logger.info("PostgreSQL tables created or verified.")
