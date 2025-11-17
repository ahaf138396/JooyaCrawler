import asyncio
import hashlib
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx
from loguru import logger

from crawler.parsing.html_extractor import extract_title, extract_text, extract_links
from crawler.storage.mongo.mongo_storage_manager import MongoStorageManager
from crawler.storage.postgres.postgres_queue_manager import PostgresQueueManager

from crawler.storage.models.page_model import CrawledPage
from crawler.storage.models.page_metadata_model import PageMetadata
from crawler.storage.models.outbound_link_model import OutboundLink
from crawler.storage.models.crawl_error_log_model import CrawlErrorLog
from crawler.storage.models.domain_crawl_policy_model import DomainCrawlPolicy


class Worker:
    def __init__(self, queue: PostgresQueueManager, mongo: MongoStorageManager, worker_id: int):
        self.queue = queue
        self.mongo = mongo
        self.worker_id = worker_id

    # ---- Respect domain rate limit ----
    async def _respect_domain_policy(self, url: str) -> None:
        parsed = urlparse(url)
        domain = parsed.netloc

        policy, _ = await DomainCrawlPolicy.get_or_create(
            domain=domain,
            defaults={"min_delay_ms": 1000},
        )

        now = datetime.now(timezone.utc)

        if policy.last_crawled_at:
            # Both are now timezone-aware → safe subtraction
            delta_ms = (now - policy.last_crawled_at).total_seconds() * 1000
            wait_ms = policy.min_delay_ms - delta_ms

            if wait_ms > 0:
                await asyncio.sleep(wait_ms / 1000)

        # Update policy state
        policy.last_crawled_at = now
        policy.crawled_today = (policy.crawled_today or 0) + 1
        await policy.save()

    # ---- Process a single URL ----
    async def process_url(self, url: str) -> None:
        try:
            await self._respect_domain_policy(url)

            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(url)
                html = response.text

            # Save raw HTML in MongoDB
            await self.mongo.save_page(url, response.status_code, html)

            # Extract text + title
            text = extract_text(html) or ""
            title = extract_title(html)

            # Store/update CrawledPage (main page table)
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

            # ---- PAGE METADATA (fixed version) ----
            content_hash = hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest() if text else None

            metadata = await PageMetadata.get_or_none(page=page)

            if metadata is None:
                metadata = await PageMetadata.create(
                    page=page,
                    html_length=len(html),
                    text_length=len(text),
                    link_count=0,
                    language=None,
                    content_hash=content_hash,
                    keywords=None
                )
            else:
                metadata.html_length = len(html)
                metadata.text_length = len(text)
                metadata.content_hash = content_hash
                await metadata.save()

            # ---- Extract links ----
            links = extract_links(url, html)
            link_count = len(links)

            # update metadata.link_count
            metadata.link_count = link_count
            await metadata.save()

            # ---- Save links + enqueue ----
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
                f"[Worker-{self.worker_id}] Crawled: {url} ({len(html)} bytes, links={link_count}, status={response.status_code})"
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

    # ---- Worker Loop ----
    async def run(self) -> None:
        logger.info(f"Worker-{self.worker_id} started.")
        while True:
            url = await self.queue.dequeue_url()
            if url:
                await self.process_url(url)
            else:
                await asyncio.sleep(2)
