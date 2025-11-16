import os
from loguru import logger
from tortoise import Tortoise


async def init_postgres():
    """
    اتصال Tortoise ORM به Postgres و ساخت/بررسی جداول.
    """

    db_user = os.getenv("POSTGRES_USER", "jooya")
    db_pass = os.getenv("POSTGRES_PASSWORD", "SuperSecurePass123")
    db_name = os.getenv("POSTGRES_DB", "jooyacrawlerdb")
    db_host = os.getenv("POSTGRES_HOST", "postgres")
    db_port = os.getenv("POSTGRES_PORT", "5432")

    db_url = f"postgres://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

    logger.info("Initializing PostgreSQL and ORM models...")

    await Tortoise.init(
        db_url=db_url,
        modules={
            "models": [
                "crawler.storage.models.queue_model",
                "crawler.storage.models.page_model",
            ]
        },
    )

    # ساخت جداول اگر وجود نداشته باشند
    await Tortoise.generate_schemas(safe=True)
    logger.info("PostgreSQL tables created or verified.")
