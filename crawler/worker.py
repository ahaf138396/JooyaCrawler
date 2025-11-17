import asyncio
import hashlib
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx
from loguru import logger

from crawler.parsing.html_extractor import (
    extract_title,
    extract_text,
    extract_links,
)
from crawler.storage.mongo.mongo_storage_manager import MongoStorageManager
from crawler.storage.postgres.postgres_queue_manager import PostgresQueueManager
from crawler.storage.models.page_model import CrawledPage
from crawler.storage.models.outbound_link_model import OutboundLink
from crawler.storage.models.page_metadata_model import PageMetadata
from crawler.storage.models.crawl_error_log_model import CrawlErrorLog
from crawler.storage.models.domain_crawl_policy_model import DomainCrawlPolicy


class Worker:
    def __init__(
        self,
        queue: PostgresQueueManager,
        mongo: MongoStorageManager,
        worker_id: int,
    ):
        self.queue = queue
        self.mongo = mongo
        self.worker_id = worker_id



    async def _respect_domain_policy(self, url: str) -> None:
        parsed = urlparse(url)
        domain = parsed.netloc

        policy, _ = await DomainCrawlPolicy.get_or_create(
            domain=domain,
            defaults={"min_delay_ms": 1000},
        )

        now = datetime.now(timezone.utc)

        if policy.last_crawled_at:
            delta_ms = (now - policy.last_crawled_at).total_seconds() * 1000
            wait_ms = policy.min_delay_ms - delta_ms
            if wait_ms > 0:
                await asyncio.sleep(wait_ms / 1000)

        policy.last_crawled_at = now
        policy.crawled_today = (policy.crawled_today or 0) + 1
        await policy.save()

    async def process_url(self, url: str) -> None:
        try:
            await self._respect_domain_policy(url)

            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(url)
                html = response.text

            # Mongo (کل HTML)
            await self.mongo.save_page(url, response.status_code, html)

            # متن خالص + عنوان
            text = extract_text(html)
            title = extract_title(html)

            # ذخیره در CrawledPage
            page, created = await CrawledPage.get_or_create(
                url=url,
                defaults={
                    "status_code": response.status_code,
                    "title": title,
                    "content": html[:5000],
                },
            )

            if not created:
                page.status_code = response.status_code
                page.title = title
                page.content = html[:5000]
                await page.save()

            # متادیتا
            content_hash = (
                hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()
                if text
                else None
            )

            await PageMetadata.get_or_create(
                page=page,
                defaults={
                    "html_length": len(html),
                    "text_length": len(text),
                    "link_count": 0,  # بعد از شمارش لینک‌ها پر می‌کنیم
                    "language": None,  # بعداً می‌تونیم language detection اضافه کنیم
                    "content_hash": content_hash,
                    "keywords": None,
                },
            )

            links = extract_links(url, html)
            link_count = len(links)

            if link_count > 0:
                # همیشه PageMetadata معتبر
                meta, _ = await PageMetadata.get_or_create(page=page)
                meta.link_count = link_count
                await meta.save()

                base_domain = urlparse(url).netloc

                for link in links[:1000]:
                    parsed = urlparse(link)
                    is_internal = parsed.netloc == base_domain

                    await OutboundLink.create(
                        source_page=page,
                        target_url=link,
                        is_internal=is_internal,
                    )

                    await self.queue.enqueue_url(link)

                logger.info(
                    f"[Worker-{self.worker_id}] Found and queued {link_count} links from {url}"
                )

            logger.info(
                f"[Worker-{self.worker_id}] Crawled: {url} ({len(html)} bytes, status={response.status_code})"
            )

        except Exception as e:
            logger.error(f"[Worker-{self.worker_id}] Error processing {url}: {e}")
            await CrawlErrorLog.create(
                url=url,
                status_code=None,
                error_message=str(e),
                worker_id=self.worker_id,
            )
            await self.queue.mark_error(url)

        else:
            await self.queue.mark_done(url)

    async def run(self) -> None:
        logger.info(f"Worker-{self.worker_id} started.")
        while True:
            url = await self.queue.dequeue_url()
            if url:
                await self.process_url(url)
            else:
                await asyncio.sleep(3)
