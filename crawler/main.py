# crawler/main.py

import asyncio
import os

import uvloop
from loguru import logger

from crawler.storage.postgres.postgres_init import init_postgres
from crawler.storage.postgres.postgres_queue_manager import PostgresQueueManager
from crawler.storage.mongo.mongo_storage_manager import MongoStorageManager
from crawler.scheduler import Scheduler
from crawler.worker import Worker


async def main():
    # اتصال ORM به Postgres و ساخت جداول
    await init_postgres()

    # ساخت صف Postgres
    queue = PostgresQueueManager()

    # اتصال به MongoDB
    mongo_uri = os.getenv(
        "MONGO_URI",
        "mongodb://jooya:SuperSecurePass123@mongo:27017/jooyacrawler",
    )
    mongo = MongoStorageManager(mongo_uri)
    await mongo.connect()

    # Scheduler
    scheduler = Scheduler(queue)

    # Workers
    workers = [Worker(queue, mongo, i).run() for i in range(3)]

    logger.info("Crawler main loop starting...")
    await asyncio.gather(scheduler.run(), *workers)


if __name__ == "__main__":
    uvloop.install()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down crawler...")
