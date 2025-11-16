import asyncio
import httpx
from loguru import logger

from crawler.storage.postgres.postgres_queue_manager import PostgresQueueManager
from crawler.storage.mongo.mongo_storage_manager import MongoStorageManager
from crawler.parsing.html_extractor import extract_title, extract_links, normalize_url
from crawler.storage.models.page_model import CrawledPage


class Worker:
    def __init__(self, queue: PostgresQueueManager, mongo: MongoStorageManager, worker_id: int):
        self.queue = queue
        self.mongo = mongo
        self.worker_id = worker_id
        self.seen: set[str] = set()  # برای جلوگیری از حلقه داخلی

    async def process_url(self, url: str):
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                res = await client.get(url)
                html = res.text

                # ذخیره خام در MongoDB
                await self.mongo.save_page(url, res.status_code, html)

                # ذخیره خلاصه در Postgres
                await CrawledPage.get_or_create(
                    url=url,
                    defaults={
                        "status_code": res.status_code,
                        "title": extract_title(html),
                        "content": html[:5000],
                    },
                )

                # استخراج لینک‌ها
                links = extract_links(url, html)
                for link in links[:1000]:  # سقف ۱۰۰۰ لینک جدید از هر صفحه
                    norm = normalize_url(link)
                    if norm in self.seen:
                        continue
                    self.seen.add(norm)
                    await self.queue.enqueue_url(norm)

                logger.info(f"[Worker-{self.worker_id}] Crawled: {url} ({len(html)} bytes)")

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
                await asyncio.sleep(3)