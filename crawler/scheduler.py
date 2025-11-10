import asyncio
from loguru import logger
from crawler.storage.postgres_queue_manager import PostgresQueueManager

class Scheduler:
    def __init__(self, queue: PostgresQueueManager):
        self.queue = queue

    async def run(self):
        logger.info("Scheduler started...")
        # این قسمت صرفاً برای تست: هر چند ثانیه یک URL جدید اضافه می‌کنیم
        counter = 0
        while True:
            url = f"https://example.com/page/{counter}"
            await self.queue.enqueue_url(url)
            logger.info(f"Added new URL to queue: {url}")
            counter += 1
            await asyncio.sleep(10)  # هر ۱۰ ثانیه یک لینک جدید
