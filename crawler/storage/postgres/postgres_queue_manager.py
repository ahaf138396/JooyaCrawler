from tortoise.transactions import in_transaction
from loguru import logger
from crawler.storage.models.queue_model import CrawlQueue


class PostgresQueueManager:
    async def enqueue_url(self, url: str):
        """
        اضافه کردن URL به صف (اگر قبلاً نباشد).
        """
        try:
            await CrawlQueue.get_or_create(url=url)
            logger.debug(f"URL enqueued: {url}")
        except Exception:
            # اگر unique violation بود، کاری نکن
            pass

    async def dequeue_url(self) -> str | None:
        """
        یک URL pending با قفل SKIP LOCKED برمی‌دارد و status را processing می‌کند.
        """
        async with in_transaction() as conn:
            rows = await conn.execute_query_dict(
                """
                SELECT * FROM crawl_queue
                WHERE status = 'pending'
                ORDER BY id
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            )

            if not rows:
                return None

            url = rows[0]["url"]

            await conn.execute_query(
                "UPDATE crawl_queue SET status='processing' WHERE url = $1",
                [url],
            )

            return url

    async def mark_done(self, url: str):
        """
        بعد از پردازش موفق URL، آن را done می‌کند.
        """
        await CrawlQueue.filter(url=url).update(status="done")
        logger.debug(f"Marked done: {url}")