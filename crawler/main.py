import asyncio
import os

from loguru import logger

try:
    import uvloop

    uvloop.install()
except ImportError:
    logger.warning("uvloop not available, using default asyncio loop.")
from crawler.monitoring.metrics_server import start_metrics_server
from crawler.storage.postgres.postgres_init import init_postgres
from crawler.storage.postgres.postgres_queue_manager import PostgresQueueManager
from crawler.storage.mongo.mongo_storage_manager import MongoStorageManager
from crawler.scheduler import Scheduler
from crawler.worker import Worker
from crawler.monitoring.metrics_server import QUEUE_PENDING
import asyncio

async def monitor_queue_size(queue: PostgresQueueManager):
    while True:
        count = await queue.count_pending()
        QUEUE_PENDING.set(count)
        await asyncio.sleep(2)

async def main() -> None:

    await init_postgres()

    queue = PostgresQueueManager()

    mongo_uri = os.getenv(
        "MONGO_URI",
        "mongodb://jooya:SuperSecurePass123@mongo:27017/jooyacrawler",
    )
    mongo = MongoStorageManager(mongo_uri)
    await mongo.connect()

    # فعلاً ثابت؛ بعداً از config/env
    seed_urls = [
        "https://fa.wikipedia.org/wiki/%D8%B5%D9%81%D8%AD%D9%87%D9%94_%D8%A7%D8%B5%D9%84%DB%8C",
    ]

    scheduler = Scheduler(queue, seed_urls, interval_seconds=10)

    workers = [Worker(queue, mongo, i).run() for i in range(20)]

    asyncio.create_task(start_metrics_server(port=8000))

    await asyncio.gather(
        scheduler.run(),
        monitor_queue_size(queue),
        *workers
    )


    await asyncio.gather(scheduler.run(), *workers)



if __name__ == "__main__":
    asyncio.run(main())
