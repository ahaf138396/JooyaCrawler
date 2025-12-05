import os
from loguru import logger
from tortoise import Tortoise

from crawler.utils.db_utils import to_asyncpg_dsn


async def init_postgres() -> None:
    """
    اتصال به PostgreSQL و ساخت/بررسی جداول.
    """
    # Radar uses `DATABASE_URL` (e.g. postgresql+psycopg2://...). Support it
    # alongside the legacy POSTGRES_* environment variables used previously in
    # the crawler so that both services can share the same database.
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        user = os.getenv("POSTGRES_USER", "jooya")
        password = os.getenv("POSTGRES_PASSWORD", "postgres")
        host = os.getenv("POSTGRES_HOST", "postgres")
        port = os.getenv("POSTGRES_PORT", "5432")
        db = os.getenv("POSTGRES_DB", "jooyacrawlerdb")

        db_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"

    db_url = to_asyncpg_dsn(db_url)




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
