import asyncio
import aiohttp
from loguru import logger
from bs4 import BeautifulSoup

from crawler.monitoring.metrics_server import (
    REQUEST_COUNT,
    REQUEST_LATENCY,
    CRAWLED_PAGES,
    FAILED_REQUESTS,
)
from crawler.utils.url_utils import normalize_url


class Worker:
    def __init__(self, queue, mongo, worker_id):
        self.queue = queue
        self.mongo = mongo
        self.session = None
        self.worker_id = worker_id

    async def fetch(self, url: str) -> str | None:
        """Fetch URL content with retries and metrics."""
        retries = 3
        timeout = aiohttp.ClientTimeout(total=10)

        for attempt in range(1, retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    self.session = session

                    with REQUEST_LATENCY.labels(worker=self.worker_id).time():
                        async with session.get(url, headers={
                            "User-Agent": "JooyaCrawler/1.0"
                        }) as resp:
                            REQUEST_COUNT.labels(worker=self.worker_id).inc()

                            if resp.status != 200:
                                FAILED_REQUESTS.labels(worker=self.worker_id).inc()
                                return None

                            return await resp.text()

            except Exception as e:
                logger.warning(
                    f"[Worker-{self.worker_id}] Fetch failed ({attempt}/3) for {url}: {e}"
                )
                await asyncio.sleep(1)

        FAILED_REQUESTS.labels(worker=self.worker_id).inc()
        return None

    async def process_url(self, url: str):
        try:
            # Duplicate check
            existing = await self.mongo.find_page(url)
            if existing is not None:
                return

            html = await self.fetch(url)
            if not html:
                return

            soup = BeautifulSoup(html, "html.parser")
            text = soup.get_text(separator=" ").strip()
            links = [normalize_url(a.get("href"), url) for a in soup.find_all("a")]
            links = [l for l in links if l]

            # Mongo save
            await self.mongo.save_page(
                url=url,
                html=html,
                text=text[:10000],  # limit for safety
                links=links,
            )

            # Save metadata
            await self.mongo.save_metadata(
                url=url,
                html_length=len(html),
                text_length=len(text),
                link_count=len(links),
            )

            # Queue discovered links
            for link in links:
                await self.queue.enqueue_url(link)

            CRAWLED_PAGES.labels(worker=self.worker_id).inc()

            logger.info(f"[Worker-{self.worker_id}] Crawled: {url}")

        except Exception as e:
            logger.error(
                f"[Worker-{self.worker_id}] Error processing {url}: {e}"
            )

    async def run(self):
        logger.info(f"Worker-{self.worker_id} started.")
        while True:
            url = await self.queue.dequeue_url()
            if not url:
                await asyncio.sleep(0.1)
                continue

            await self.process_url(url)
