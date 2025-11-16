import asyncio
import httpx
from loguru import logger
from crawler.storage.postgres_queue_manager import PostgresQueueManager

from bs4 import BeautifulSoup

def extract_title(html: str) -> str:
    try:
        soup = BeautifulSoup(html, "html.parser")
        title_tag = soup.find("title")
        return title_tag.text.strip() if title_tag else None
    except Exception:
        return None


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
                from crawler.storage.models.page_model import CrawledPage

                # ذخیره در دیتابیس
                await CrawledPage.get_or_create(
                    url=url,
                    defaults={
                        "status_code": response.status_code,
                        "title": extract_title(content),
                        "content": content[:5000],  # برای MVP فقط ۵۰۰۰ کاراکتر
                    },
                )


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
