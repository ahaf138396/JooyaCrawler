import asyncio
import datetime as dt
from typing import Optional
from tortoise import Tortoise
from loguru import logger
from crawler.storage.models.queue_model import CrawlQueue

from urllib.parse import urlsplit, urlunsplit, urldefrag

def normalize_url(url: str) -> str:
    # fragment مثل #bodyContent حذف می‌شود
    defragged, _ = urldefrag(url)

    parts = urlsplit(defragged)
    scheme = parts.scheme.lower()
    netloc = parts.netloc.lower()
    path = parts.path or "/"

    # / آخر را برای مسیرهای غیر روت حذف کن (اختیاری ولی خوبه)
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    return urlunsplit((scheme, netloc, path, parts.query, ""))

_dequeue_lock = asyncio.Lock()


class PostgresQueueManager:

    async def enqueue_url(self, url: str, priority: int = 0) -> None:
        try:
            url1 = normalize_url(url)
            obj, created = await CrawlQueue.get_or_create(
                url = url1,
                defaults={"priority": priority, "status": "pending"},
            )

            if not created:
                # اگر همین الان تو صفه یا در حال پردازش، کاری نکن
                if obj.status in ("pending", "processing"):
                    return

                # فعلاً صفحات done رو دوباره وارد صف نکن
                if obj.status == "done":
                    return

                # برای error (با محدودیت تعداد تلاش) اجازه‌ی retry داشته باشیم
                if obj.status == "error":
                    if obj.error_count is not None and obj.error_count >= 3:
                        # از حد تلاش گذشتیم، دیگه رهاش کن
                        return
                    obj.status = "pending"
                    obj.priority = priority
                    obj.scheduled_at = dt.datetime.now(dt.timezone.utc)
                    await obj.save()

            logger.debug("Enqueued URL: %s (priority=%s)", url1, priority)
        except Exception as e:
            logger.error("Error enqueuing URL %s: %s", url, e)

    async def dequeue_url(self) -> Optional[str]:
        conn = Tortoise.get_connection("default")
        sql = """
        WITH cte AS (
            SELECT id
            FROM crawl_queue
            WHERE status = 'pending'
            ORDER BY priority DESC, scheduled_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        UPDATE crawl_queue AS cq
        SET status = 'processing',
            last_attempt_at = NOW()
        FROM cte
        WHERE cq.id = cte.id
        RETURNING cq.url;
        """
        rows = await conn.execute_query_dict(sql)
        if not rows:
            return None

        url = rows[0]["url"]
        logger.debug("Dequeued: %s", url)
        return url

    async def mark_done(self, url: str) -> None:
        url = normalize_url(url)
        await CrawlQueue.filter(url=url).update(status="done")

    async def mark_error(self, url: str) -> None:
        url = normalize_url(url)
        await CrawlQueue.filter(url=url).update(
            status="error",
            error_count=1 + (await CrawlQueue.get(url=url)).error_count,
        )

    async def has_pending(self) -> bool:
        return await CrawlQueue.filter(status="pending").exists()

    async def count_pending(self) -> int:
        return await CrawlQueue.filter(status="pending").count()
