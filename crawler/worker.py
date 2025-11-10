import asyncio
import httpx
from loguru import logger
from crawler.storage.postgres_queue_manager import PostgresQueueManager

class Worker:
    def __init__(self, queue: PostgresQueueManager, worker_id: int):
        self.queue = queue
        self.worker_id = worker_id

    async def process_url(self, url: str):
        """اینجا بعداً محتوای صفحه رو دانلود و تحلیل می‌کنیم"""
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(url)
                logger.info(f"[Worker-{self.worker_id}] Crawled: {url} ({len(response.text)} bytes)")
        except Exception as e:
            logger.error(f"[Worker-{self.worker_id}] Error processing {url}: {e}")
        finally:
            await self.queue.mark_done(url)

    async def run(self):
        logger.info(f"Worker-{self.worker_id} started.")
        while True:
            url = await self.queue.dequeue_url()
            if url:
                await self.process_url(url)
            else:
                await asyncio.sleep(3)  # اگر صف خالی بود
