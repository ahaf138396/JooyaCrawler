from loguru import logger
from crawler.storage.models.queue_model import CrawlQueue

class PostgresQueueManager:
    async def enqueue_url(self, url: str):
        await CrawlQueue.create(url=url)
        logger.debug(f"URL enqueued: {url}")

    async def dequeue_url(self):
        task = await CrawlQueue.filter(status="pending").order_by("added_at").first()
        if task:
            task.status = "processing"
            await task.save()
            logger.debug(f"Dequeued: {task.url}")
            return task.url
        return None

    async def mark_done(self, url: str):
        task = await CrawlQueue.filter(url=url).first()
        if task:
            task.status = "done"
            await task.save()
            logger.debug(f"Marked done: {url}")
