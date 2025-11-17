import datetime as dt
from typing import Optional

from loguru import logger
from crawler.storage.models.queue_model import CrawlQueue


class PostgresQueueManager:
    async def enqueue_url(self, url: str, priority: int = 0) -> None:
        """
        اضافه کردن URL به صف (اگر وجود داشته باشد، دوباره pending اش می‌کنیم).
        """
        obj, created = await CrawlQueue.get_or_create(
            url=url,
            defaults={"priority": priority, "status": "pending"},
        )

        if not created and obj.status in ("done", "error"):
            obj.status = "pending"
            obj.priority = priority
            obj.error_count = 0
            await obj.save()

        logger.debug(f"URL enqueued: {url}")

    async def dequeue_url(self) -> Optional[str]:
        """
        برداشتن قدیمی‌ترین URL pending با بالاترین priority.
        """
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
