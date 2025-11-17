import os
from loguru import logger
from tortoise import Tortoise


async def init_postgres() -> None:
    """
    اتصال به PostgreSQL و ساخت/بررسی جداول.
    """
    user = os.getenv("POSTGRES_USER", "jooya")
    password = os.getenv("POSTGRES_PASSWORD", "SuperSecurePass123")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "jooyacrawlerdb")

    db_url = f"postgres+asyncpg://{user}:{password}@{host}:{port}/{db}"



    logger.info("Initializing PostgreSQL and ORM models...")

    await Tortoise.init(
        db_url=db_url,
        modules={
            "models": [
                "crawler.storage.models.queue_model",
                "crawler.storage.models.page_model",
                "crawler.storage.models.outbound_link_model",
                "crawler.storage.models.page_metadata_model",
                "crawler.storage.models.crawl_error_log_model",
                "crawler.storage.models.domain_crawl_policy_model",
            ]
        },
    )

    await Tortoise.generate_schemas(safe=True)
    logger.info("PostgreSQL tables created or verified.")
