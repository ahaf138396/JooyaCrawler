import asyncio
import os
from loguru import logger

# -------------------------------
# UVLOOP (اگر نصب بود فعال می‌کنیم)
# -------------------------------
try:
    import uvloop
    uvloop.install()
except ImportError:
    logger.warning("uvloop not available, using default asyncio loop.")

# -------------------------------
# INTERNAL IMPORTS
# -------------------------------
from crawler.monitoring.metrics_server import start_metrics_server, QUEUE_PENDING
from crawler.storage.postgres.postgres_init import init_postgres
from crawler.storage.postgres.postgres_queue_manager import PostgresQueueManager
from crawler.storage.mongo.mongo_storage_manager import MongoStorageManager
from crawler.scheduler import Scheduler
from crawler.worker import Worker


# -------------------------------
# QUEUE METRIC MONITOR TASK
# -------------------------------
async def monitor_queue_size(queue: PostgresQueueManager):
    while True:
        try:
            count = await queue.count_pending()
            QUEUE_PENDING.set(count)
        except Exception as e:
            logger.error(f"Queue monitor error: {e}")
        await asyncio.sleep(2)


# -------------------------------
# MAIN APPLICATION
# -------------------------------
async def main() -> None:

    logger.info("Starting crawler system...")

    # ---- PostgreSQL ----
    await init_postgres()
    queue = PostgresQueueManager()

    # ---- MongoDB ----
    mongo_uri = os.getenv(
        "MONGO_URI",
        "mongodb://jooya:SuperSecurePass123@mongo:27017/jooyacrawlerdb?authSource=admin",
    )

    mongo = MongoStorageManager(mongo_uri)
    await mongo.connect()

    # ---- Seed URLs ----
    seed_urls = [
        "https://example.com",
    ]
    scheduler = Scheduler(queue, seed_urls, interval_seconds=10)

    # ---- Worker Pool ----
    WORKER_COUNT = int(os.getenv("WORKERS", 3))
    workers = [Worker(queue, mongo, i).run() for i in range(WORKER_COUNT)]

    # ---- Metrics Server ----
    asyncio.create_task(start_metrics_server(port=8000))

    # ---- Queue Metric Monitor ----
    asyncio.create_task(monitor_queue_size(queue))

    # ---- Run Main Tasks Concurrently ----
    logger.info("Crawler system started successfully.")
    await asyncio.gather(
        scheduler.run(),
        *workers,
    )


# -------------------------------
# ENTRYPOINT
# -------------------------------
if __name__ == "__main__":
    asyncio.run(main())
