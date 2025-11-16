import asyncio
import os
from loguru import logger

from crawler.storage.postgres.postgres_queue_manager import PostgresQueueManager


class Scheduler:
    def __init__(self, queue: PostgresQueueManager):
        self.queue = queue
        # لیست اولیه seed ها را از env بخوان، اگر نبود مثال پیش‌فرض
        seeds_env = os.getenv("CRAWLER_SEEDS")
        if seeds_env:
            self.seeds = [s.strip() for s in seeds_env.split(",") if s.strip()]
        else:
            self.seeds = ["https://example.com"]

    async def _seed_once(self):
        for url in self.seeds:
            await self.queue.enqueue_url(url)
            logger.info(f"Seed added to queue: {url}")

    async def run(self):
        """
        فعلاً ساده: یک بار seed، و بعد هر ۱۰ ثانیه یک URL جدید ساختگی (برای تست).
        بعداً می‌تونیم تبدیلش کنیم به منطق واقعی (RSS، sitemap، لیست دامنه‌ها و ...).
        """
        logger.info("Scheduler started...")

        # یک بار seed اولیه
        await self._seed_once()

        i = 0
        while True:
            # فعلا برای تست، هر ۱۰ ثانیه یک URL ساختگی جدید
            test_url = f"{self.seeds[0].rstrip('/')}/page/{i}"
            await self.queue.enqueue_url(test_url)
            logger.info(f"Added new URL to queue: {test_url}")
            i += 1
            await asyncio.sleep(10)