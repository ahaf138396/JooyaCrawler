import asyncio
import traceback
import httpx
from loguru import logger

from crawler.monitoring.metrics_server import (
    WORKER_PROCESSED,
    WORKER_ACTIVE,
)
from crawler.storage.models import CrawlErrorLog
from crawler.storage.mongo.mongo_storage_manager import MongoStorageManager
from crawler.storage.postgres.postgres_queue_manager import PostgresQueueManager


class Worker:
    def __init__(self, queue: PostgresQueueManager, mongo: MongoStorageManager, worker_id: int):
        self.queue = queue
        self.mongo = mongo
        self.worker_id = worker_id
        self.running = True

        # هر Worker یک Active Flag برای Prometheus دارد
        WORKER_ACTIVE.labels(worker_id=self.worker_id).set(1)

        # timeout کاملاً استاندارد
        self.timeout = httpx.Timeout(
            timeout=12.0,
            connect=12.0,
            read=12.0,
            write=12.0,
            pool=12.0,
        )

        self.client = httpx.AsyncClient(
            timeout=self.timeout,
            follow_redirects=True,   # برای سایت‌های ریدایرکت
            headers={
                "User-Agent": "JooyaCrawler/1.0 (+https://jooya.ai)"
            }
        )

    async def fetch(self, url: str):
        try:
            response = await self.client.get(url)

            if response.status_code >= 400:
                raise Exception(f"HTTP {response.status_code}")

            return response.text, response.status_code

        except httpx.TimeoutException:
            raise Exception("Timeout while fetching URL")

        except httpx.ConnectError:
            raise Exception("Connection failed")

        except Exception as e:
            raise Exception(f"Network error: {str(e)}")

    async def process_url(self, url: str):
        try:
            html, status_code = await self.fetch(url)

            # ذخیره در Mongo
            await self.mongo.store_page(url, html, status_code)

            # Mark Done
            await self.queue.mark_done(url)

            WORKER_PROCESSED.labels(worker_id=self.worker_id).inc()
            logger.info(f"[Worker-{self.worker_id}] DONE: {url}")



        except Exception as e:
            tb = traceback.format_exc()

            logger.error(
                f"[Worker-{self.worker_id}] ERROR processing {url}\n"
                f"Exception: {e}\n"
                f"Traceback:\n{tb}"
            )

            await CrawlErrorLog.create(
                url=url,
                status_code=None,
                error_message=f"{e}\n{tb}",
                worker_id=self.worker_id,
            )

            await self.queue.mark_error(url)

            '''
                    except Exception as e:
            WORKER_FAILED.labels(worker_id=self.worker_id).inc()
            
            await self.queue.mark_error(url, str(e), self.worker_id)

            logger.error(f"[Worker-{self.worker_id}] Error processing {url}: {e}")
            '''

    async def run(self):
        logger.info(f"Worker-{self.worker_id} started.")

        while self.running:
            item = await self.queue.dequeue_url()

            if not item:
                await asyncio.sleep(0.5)
                continue

            url = item.url
            logger.debug(f"[Worker-{self.worker_id}] Dequeued: {url}")

            await self.process_url(url)
