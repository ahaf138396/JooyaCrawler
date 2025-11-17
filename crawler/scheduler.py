import asyncio
from typing import List

from loguru import logger

from crawler.storage.models.queue_model import CrawlQueue
from crawler.storage.postgres.postgres_queue_manager import PostgresQueueManager


class Scheduler:
    def __init__(
        self,
        queue: PostgresQueueManager,
        seed_urls: List[str],
        interval_seconds: int = 10,
    ):
        self.queue = queue
        self.seed_urls = seed_urls
        self.interval_seconds = interval_seconds

    async def _seed_if_empty(self) -> None:
        has_pending = await self.queue.has_pending()
        if not has_pending:
            for url in self.seed_urls:
                await self.queue.enqueue_url(url)
                logger.info(f"Added seed URL to queue: {url}")

    async def run(self) -> None:
        logger.info("Scheduler started...")
        while True:
            await self._seed_if_empty()
            await asyncio.sleep(self.interval_seconds)
