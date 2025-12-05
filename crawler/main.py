import asyncio
import os
import signal
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
from crawler.storage.mongo.mongo_storage_manager import MongoStorageManager
from tortoise import Tortoise

from crawler.storage.radar_queue_manager import RadarQueueManager
from crawler.worker import Worker


# -------------------------------
# QUEUE METRIC MONITOR TASK
# -------------------------------
async def monitor_queue_size(queue: RadarQueueManager):
    while True:
        try:
            count = await queue.count_scheduled()
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
    queue = RadarQueueManager()
    await queue.connect()

    # ---- MongoDB ----
    mongo_uri = os.getenv(
        "MONGO_URI",
        "mongodb://localhost:27017/jooyacrawlerdb",
    )

    mongo = MongoStorageManager(mongo_uri)
    await mongo.connect()

    # ---- Worker Pool ----
    WORKER_COUNT = int(os.getenv("WORKERS", 12))
    worker_tasks = [asyncio.create_task(Worker(queue, mongo, i).run()) for i in range(WORKER_COUNT)]

    # ---- Metrics Server ----
    await start_metrics_server(port=8000)

    # ---- Queue Metric Monitor ----
    monitor_task = asyncio.create_task(monitor_queue_size(queue))

    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)

    logger.info("Crawler system started successfully.")

    try:
        await shutdown_event.wait()
    except asyncio.CancelledError:
        pass
    finally:
        monitor_task.cancel()
        for task in worker_tasks:
            task.cancel()

        await asyncio.gather(monitor_task, *worker_tasks, return_exceptions=True)

        await queue.close()
        await mongo.close()
        await Tortoise.close_connections()


# -------------------------------
# ENTRYPOINT
# -------------------------------
if __name__ == "__main__":
    asyncio.run(main())
