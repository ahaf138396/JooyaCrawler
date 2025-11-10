from tortoise import Tortoise
from loguru import logger

DB_URL = "postgres://jooya:SuperSecurePass123@postgres:5432/jooyacrawler"

async def init_postgres():
    logger.info("Initializing PostgreSQL and ORM models...")
    await Tortoise.init(
        db_url=DB_URL,
        modules={"models": ["crawler.storage.models.queue_model"]}
    )
    await Tortoise.generate_schemas(safe=True)
    logger.success("✅ PostgreSQL tables created or verified.")
