import asyncio
import hashlib
import time
from datetime import datetime, timezone
from typing import Optional
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

from crawler.monitoring.metrics_server import (
    REQUEST_COUNT,
    FAILED_REQUESTS,
    CRAWLED_PAGES,
    REQUEST_LATENCY,
    WORKER_PROCESSED,
    WORKER_FAILED,
    WORKER_ACTIVE,
)


MAX_PARSE_BYTES = 500_000
MAX_LINKS_PER_PAGE = 1000
MAX_LINKS_HEAVY_PAGE = 200


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
        self.name = f"Worker-{worker_id}"
        self.client: Optional[httpx.AsyncClient] = None

    # --------------------------
    #  Domain crawl policy
    # --------------------------
    async def _respect_domain_policy(self, url: str) -> None:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()

        policy, _ = await DomainCrawlPolicy.get_or_create(
            domain=domain,
            defaults={
                "min_delay_ms": 1000,
                "last_crawled_at": None,
                "crawled_today": 0,
            },
        )

        now = datetime.now(timezone.utc)

        if policy.last_crawled_at is not None:
            delta_ms = (now - policy.last_crawled_at).total_seconds() * 1000
            wait_ms = policy.min_delay_ms - delta_ms
            if wait_ms > 0:
                await asyncio.sleep(wait_ms / 1000)

        policy.last_crawled_at = now
        policy.crawled_today = (policy.crawled_today or 0) + 1
        await policy.save()

    # --------------------------
    #  HTTP fetch with metrics
    # --------------------------
    async def _fetch(self, url: str) -> httpx.Response:
        worker_label = str(self.worker_id)
        REQUEST_COUNT.labels(worker=worker_label).inc()

        start = time.perf_counter()
        if self.client is None:
            raise RuntimeError("HTTP client is not initialized")
        try:
            resp = await self.client.get(
                url,
                headers={
                    "User-Agent": "JooyaCrawler/0.1 (+https://example.com)",
                    "Accept": (
                        "text/html,application/xhtml+xml,application/xml;q=0.9,"
                        "*/*;q=0.8"
                    ),
                    "Accept-Language": "fa-IR,fa;q=0.9,en-US;q=0.8,en;q=0.7",
                },
            )
            return resp
        finally:
            elapsed = time.perf_counter() - start
            REQUEST_LATENCY.labels(worker=worker_label).observe(elapsed)

    # --------------------------
    #  Main processing
    # --------------------------
    async def process_url(self, url: str) -> None:
        worker_label = str(self.worker_id)

        try:
            await self._respect_domain_policy(url)

            response = await self._fetch(url)

            status_code = response.status_code
            html = response.text or ""

            parse_source = html
            heavy_page = len(html) > MAX_PARSE_BYTES
            if heavy_page:
                parse_source = html[:MAX_PARSE_BYTES]
                logger.debug(
                    f"[{self.name}] Large page detected ({len(html)} bytes); "
                    f"parsing first {MAX_PARSE_BYTES} bytes"
                )

            # ذخیره نسخه خام در Mongo
            await self.mongo.save_page(url, status_code, html)

            # اگر خطای HTTP یا بدنه خالی بود، لاگ خطا و علامت‌گذاری error
            if status_code >= 400 or not html.strip():
                FAILED_REQUESTS.labels(worker=worker_label).inc()
                raise Exception(f"Non-success status code: {status_code}")

            # Parsing
            text = extract_text(parse_source) or ""
            title = extract_title(parse_source) or ""

            # Hash برای تشخیص تغییر محتوا
            content_for_hash = text if text else html
            content_hash = hashlib.sha256(
                content_for_hash.encode("utf-8", errors="ignore")
            ).hexdigest()

            # ذخیره / آپدیت CrawledPage
            page, created = await CrawledPage.get_or_create(
                url=url,
                defaults={
                    "status_code": status_code,
                    "title": title,
                    "content": html[:5000],
                },
            )

            if not created:
                page.status_code = status_code
                page.title = title
                page.content = html[:5000]
                await page.save()

            # متادیتا (بدون هیچ Truth-Value روی relation ها)
            html_length = len(html)
            text_length = len(text)

            meta, meta_created = await PageMetadata.get_or_create(
                page=page,
                defaults={
                    "html_length": html_length,
                    "text_length": text_length,
                    "link_count": 0,
                    "language": None,
                    "content_hash": content_hash,
                    "keywords": None,
                },
            )

            if not meta_created:
                meta.html_length = html_length
                meta.text_length = text_length
                meta.content_hash = content_hash
                # link_count را بعد از استخراج لینک‌ها به‌روزرسانی می‌کنیم
                await meta.save()

            # لینک‌ها
            links = extract_links(url, parse_source) or []
            base_domain = urlparse(url).netloc.lower()

            # حذف تکراری‌ها
            unique_links = []
            seen = set()
            for link in links:
                if not link:
                    continue
                if link in seen:
                    continue
                seen.add(link)
                unique_links.append(link)

            link_count = len(unique_links)
            max_links_to_queue = (
                MAX_LINKS_HEAVY_PAGE if heavy_page else MAX_LINKS_PER_PAGE
            )

            if link_count > 0:
                # به‌روزرسانی link_count در متادیتا
                meta.link_count = link_count
                await meta.save()

                # ذخیره OutboundLink و صف کردن لینک‌های جدید
                links_to_queue = unique_links[:max_links_to_queue]
                for link in links_to_queue:
                    parsed = urlparse(link)
                    is_internal = parsed.netloc.lower() == base_domain

                    await OutboundLink.create(
                        source_page=page,
                        target_url=link,
                        is_internal=is_internal,
                    )

                    await self.queue.enqueue_url(link)

                logger.info(
                    f"[{self.name}] Found {link_count} links from {url} "
                    f"(queued up to {len(links_to_queue)})"
                )

            # Metrics
            CRAWLED_PAGES.labels(worker=worker_label).inc()
            WORKER_PROCESSED.labels(worker_id=worker_label).inc()

            logger.info(
                f"[{self.name}] Crawled: {url} ({html_length} bytes, status={status_code})"
            )

            await self.queue.mark_done(url)

        except Exception as e:
            logger.error(f"[{self.name}] Error processing {url}: {e}")
            WORKER_FAILED.labels(worker_id=worker_label).inc()

            await CrawlErrorLog.create(
                url=url,
                status_code=None,
                error_message=str(e),
                worker_id=self.worker_id,
            )
            await self.queue.mark_error(url)

    # --------------------------
    #  Worker loop
    # --------------------------
    async def run(self) -> None:
        worker_label = str(self.worker_id)
        WORKER_ACTIVE.labels(worker_id=worker_label).set(1.0)

        logger.info(f"{self.name} started.")

        timeout = httpx.Timeout(timeout=20.0, connect=5.0)

        try:
            async with httpx.AsyncClient(
                    timeout=timeout,
                    follow_redirects=True,
            ) as client:
                self.client = client

                while True:
                    url = await self.queue.dequeue_url()
                    if url is None:
                        await asyncio.sleep(3)
                        continue

                    logger.debug(f"[{self.name}] Dequeued: {url}")
                    await self.process_url(url)
        finally:
            self.client = None
            WORKER_ACTIVE.labels(worker_id=worker_label).set(0.0)

