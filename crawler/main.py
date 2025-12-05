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
from crawler.utils.config_loader import load_config
from crawler.utils.env_loader import load_environment
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

    load_environment()
    config = load_config()

    metrics_runner = None
    metrics_site = None

    # ---- PostgreSQL ----
    await init_postgres()
    max_depth_env = os.getenv("MAX_DEPTH") or config.max_depth
    max_depth = None
    if max_depth_env is not None:
        try:
            max_depth = int(max_depth_env)
            logger.info(f"Max crawl depth set to {max_depth}")
        except ValueError:
            logger.warning(
                f"Ignoring invalid MAX_DEPTH value '{max_depth_env}'; proceeding without limit."
            )

    max_pages_env = os.getenv("MAX_PAGES") or config.max_pages
    max_pages = None
    if max_pages_env is not None:
        try:
            max_pages = int(max_pages_env)
            logger.info(f"Max pages limit set to {max_pages}")
        except ValueError:
            logger.warning(
                f"Ignoring invalid MAX_PAGES value '{max_pages_env}'; proceeding without limit."
            )

    queue = RadarQueueManager(max_depth=max_depth, max_pages=max_pages)
    await queue.connect()

    # ---- MongoDB ----
    mongo_uri = os.getenv("MONGO_URI") or os.getenv("MONGO_URL") or config.mongo_url
    mongo_db = os.getenv("MONGO_DB") or config.mongo_db
    max_html_bytes_env = os.getenv("MAX_SAVED_HTML_BYTES")
    max_html_bytes = int(max_html_bytes_env) if max_html_bytes_env else 500_000

    mongo = MongoStorageManager(
        mongo_uri,
        db_name=mongo_db,
        max_html_bytes=max_html_bytes,
    )
    await mongo.connect()

    # ---- Worker Pool ----
    WORKER_COUNT = int(os.getenv("WORKERS", 12))
    worker_tasks = [asyncio.create_task(Worker(queue, mongo, i).run()) for i in range(WORKER_COUNT)]

    # ---- Metrics Server ----
    metrics_runner, metrics_site = await start_metrics_server(port=8000)

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

        if metrics_runner is not None:
            await metrics_runner.shutdown()
            await metrics_runner.cleanup()

        await queue.close()
        await mongo.close()
        await Tortoise.close_connections()


# -------------------------------
# ENTRYPOINT
# -------------------------------
if __name__ == "__main__":
    asyncio.run(main())
