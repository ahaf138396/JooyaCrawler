import asyncio
import time
from urllib.parse import urlparse
from loguru import logger

class Scheduler:
    def __init__(self, redis, postgres, config):
        self.redis = redis
        self.postgres = postgres
        self.config = config
        self.domain_last_crawl = {}  # نگهداری زمان آخرین خزش هر دامنه برای crawl_delay

    async def run(self):
        logger.info("Scheduler started...")
        while True:
            try:
                await self.schedule_new_urls()
                await asyncio.sleep(2)  # تنظیم برای جلوگیری از مصرف CPU زیاد
            except Exception as e:
                logger.exception(f"Scheduler error: {e}")
                await asyncio.sleep(5)

    async def schedule_new_urls(self):
        # دریافت تعدادی URL از دیتابیس یا منبع اولیه
        urls = await self.postgres.get_unscheduled_urls(limit=20)
        if not urls:
            return

        for url in urls:
            # بررسی تکرار در Redis (visited)
            if await self.redis.is_visited(url):
                continue

            # بررسی محدودیت زمانی دامنه (crawl_delay)
            if not await self._can_crawl_now(url):
                continue

            # اضافه کردن به صف Redis
            await self.redis.enqueue_url(url)
            await self.postgres.mark_as_scheduled(url)
            logger.debug(f"Scheduled URL: {url}")

    async def _can_crawl_now(self, url: str) -> bool:
        """بررسی زمان مجاز برای خزش دامنه"""
        parsed = urlparse(url)
        domain = parsed.netloc
        delay = self.config.crawl_delay_default

        last_time = self.domain_last_crawl.get(domain, 0)
        now = time.time()
        if now - last_time >= delay:
            self.domain_last_crawl[domain] = now
            return True
        return False
