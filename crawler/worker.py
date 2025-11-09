import asyncio
import httpx
from loguru import logger
from crawler.parser import Parser
from urllib.parse import urlparse, urljoin

class Worker:
    def __init__(self, redis, postgres, mongo, config):
        self.redis = redis
        self.postgres = postgres
        self.mongo = mongo
        self.config = config
        self.parser = Parser()

    async def run(self):
        logger.info("Worker started...")
        workers = [asyncio.create_task(self._worker_loop(i)) for i in range(self.config.crawler_workers)]
        await asyncio.gather(*workers)

    async def _worker_loop(self, wid: int):
        """اجرای مداوم Workerها"""
        async with httpx.AsyncClient(timeout=self.config.request_timeout) as client:
            while True:
                try:
                    url = await self.redis.dequeue_url()
                    if not url:
                        await asyncio.sleep(1)
                        continue

                    if await self.redis.is_visited(url):
                        continue

                    logger.debug(f"[Worker-{wid}] Crawling: {url}")

                    # دانلود صفحه
                    html, status = await self._fetch(client, url)
                    if not html:
                        continue

                    # ذخیره محتوا
                    await self.mongo.save_page(url, html)
                    await self.postgres.mark_as_crawled(url, status)
                    await self.redis.add_to_visited(url)

                    # استخراج لینک‌ها
                    links = self.parser.extract_links(url, html)
                    for link in links:
                        if not await self.redis.is_visited(link):
                            await self.redis.enqueue_url(link)

                    logger.debug(f"[Worker-{wid}] Done: {url}, Found {len(links)} new links")

                    await asyncio.sleep(self.config.crawl_delay_default)

                except Exception as e:
                    logger.exception(f"[Worker-{wid}] Error: {e}")
                    await asyncio.sleep(3)

    async def _fetch(self, client, url: str):
        """دانلود صفحه"""
        try:
            response = await client.get(url, headers={"User-Agent": "JooyaCrawler/1.0"})
            if response.status_code == 200:
                return response.text, 200
            else:
                logger.warning(f"Non-200: {response.status_code} for {url}")
                return None, response.status_code
        except httpx.RequestError as e:
            logger.error(f"Request failed for {url}: {e}")
            return None, 0
