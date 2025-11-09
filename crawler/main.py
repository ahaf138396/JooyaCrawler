import asyncio
from loguru import logger
from crawler.utils.config_loader import load_config
from crawler.storage.redis_manager import RedisManager
from crawler.storage.postgres_manager import PostgresManager
from crawler.storage.mongo_manager import MongoManager
from crawler.scheduler import Scheduler
from crawler.worker import Worker

async def main():
    # بارگذاری تنظیمات
    config = load_config()
    logger.info("Configuration loaded successfully")

    # اتصال به دیتابیس‌ها
    redis = RedisManager(config.redis_url)
    postgres = PostgresManager(config.postgres_url)
    mongo = MongoManager(config.mongo_url)

    await redis.connect()
    await postgres.connect()
    await mongo.connect()

    logger.info("All databases connected successfully")

    # راه‌اندازی Scheduler و Worker
    scheduler = Scheduler(redis, postgres, config)
    worker = Worker(redis, postgres, mongo, config)

    # اجرای موازی (Scheduler + Workers)
    await asyncio.gather(
        scheduler.run(),
        worker.run()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Shutting down gracefully...")
