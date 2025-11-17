import datetime as dt
from typing import Optional

from loguru import logger
from crawler.storage.models.queue_model import CrawlQueue


class PostgresQueueManager:
    async def enqueue_url(self, url: str, priority: int = 0) -> None:
        obj = await CrawlQueue.get_or_none(url=url)

        if obj is None:
            await CrawlQueue.create(url=url, priority=priority, status="pending")
            return

        # اگر در وضعیت processing گیر کرده بود → آزاد کن
        if obj.status in ("processing", "done", "error"):
            await CrawlQueue.filter(id=obj.id).update(
                status="pending",
                error_count=0,
                last_attempt_at=None
            )

    async def dequeue_url(self) -> Optional[str]:
        item = (
            await CrawlQueue.filter(status="pending")
            .order_by("-priority", "id")
            .first()
        )

        if not item:
            return None

        item.status = "processing"
        item.last_attempt_at = dt.datetime.utcnow()
        await item.save()

        logger.debug(f"Dequeued: {item.url}")
        return item.url

    async def mark_done(self, url: str) -> None:
        await CrawlQueue.filter(url=url).update(status="done")

    async def mark_error(self, url: str) -> None:
        await CrawlQueue.filter(url=url).update(
            status="error",
            error_count=1 + (await CrawlQueue.get(url=url)).error_count,
        )

    async def has_pending(self) -> bool:
        return await CrawlQueue.filter(status="pending").exists()

    async def count_pending(self) -> int:
        return await CrawlQueue.filter(status="pending").count()
