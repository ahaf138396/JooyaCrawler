import asyncio
import httpx
from loguru import logger
from crawler.storage.postgres_queue_manager import PostgresQueueManager
from urllib.parse import urljoin, urlparse
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
                        "content": content[:5000],
                    },
                )

                links = extract_links(url, content)
                if links:
                    for link in links[:1000]:
                        await queue_manager.enqueue_url(link)
                    logger.info(f"[Worker-{worker_id}] Found and queued {len(links)} new links from {url}")



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


def extract_links(base_url: str, html: str) -> list[str]:
    links = set()
    base_domain = urlparse(base_url).netloc

    try:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            full_url = urljoin(base_url, href)
            parsed = urlparse(full_url)

            # فقط لینک‌های http/https و داخل دامنه اصلی
            if parsed.scheme in ("http", "https") and base_domain in parsed.netloc:
                links.add(full_url)
    except Exception as e:
        logger.warning(f"Failed to extract links from {base_url}: {e}")

    return list(links)
