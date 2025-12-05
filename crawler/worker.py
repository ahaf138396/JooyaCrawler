import asyncio
import hashlib
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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
from crawler.storage.radar_queue_manager import FrontierTask, RadarQueueManager

from crawler.storage.models.page_model import CrawledPage
from crawler.storage.models.outbound_link_model import OutboundLink
from crawler.storage.models.page_metadata_model import PageMetadata
from crawler.storage.models.crawl_error_log_model import CrawlErrorLog
from crawler.storage.models.domain_crawl_policy_model import DomainCrawlPolicy
from tortoise.transactions import in_transaction

from crawler.monitoring.metrics_server import (
    REQUEST_COUNT,
    FAILED_REQUESTS,
    CRAWLED_PAGES,
    REQUEST_LATENCY,
    WORKER_PROCESSED,
    WORKER_FAILED,
    WORKER_ACTIVE,
    ROBOTS_SKIPPED,
    SKIPPED_NON_HTML,
    SKIPPED_LARGE_BODIES,
    SKIPPED_LINKS,
)
from crawler.utils.config_loader import load_config
from crawler.utils.filters import is_valid_link
from crawler.utils.url_utils import get_domain
from crawler.utils.robots import RobotsHandler


MAX_PARSE_BYTES = 500_000
MAX_DOWNLOAD_BYTES = int(os.getenv("MAX_DOWNLOAD_BYTES", 2_000_000))
MAX_LINKS_PER_PAGE = 1000
MAX_LINKS_HEAVY_PAGE = 200


@dataclass
class FetchResult:
    status_code: int
    content: str
    content_type: str
    skipped: bool
    skip_reason: Optional[str] = None


class Worker:
    def __init__(
        self,
        queue: RadarQueueManager,
        mongo: MongoStorageManager,
        worker_id: int,
    ):
        self.queue = queue
        self.mongo = mongo
        self.worker_id = worker_id
        self.name = f"Worker-{worker_id}"
        self.client: Optional[httpx.AsyncClient] = None
        config = load_config()
        self.user_agent = config.crawler_user_agent
        self.default_min_delay_ms = int(config.crawl_delay_default * 1000)
        self.robots_handler: Optional[RobotsHandler] = None

    # --------------------------
    #  Domain crawl policy
    # --------------------------
    async def _respect_domain_policy(self, url: str) -> None:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        now = datetime.now(timezone.utc)

        async with in_transaction() as conn:
            policy = (
                await DomainCrawlPolicy.filter(domain=domain)
                .using_db(conn)
                .select_for_update()
                .first()
            )

            if policy is None:
                policy = await DomainCrawlPolicy.using_db(conn).create(
                    domain=domain,
                    min_delay_ms=self.default_min_delay_ms,
                    last_crawled_at=None,
                    next_allowed_at=None,
                    crawled_today=0,
                )

            now = datetime.now(timezone.utc)
            last_crawled = policy.last_crawled_at
            next_allowed = policy.next_allowed_at

            if last_crawled and last_crawled.tzinfo is None:
                last_crawled = last_crawled.replace(tzinfo=timezone.utc)
            if next_allowed and next_allowed.tzinfo is None:
                next_allowed = next_allowed.replace(tzinfo=timezone.utc)

            if last_crawled is None or last_crawled.date() != now.date():
                policy.crawled_today = 0

            min_delay_wait = 0
            if last_crawled:
                delta_ms = (now - last_crawled).total_seconds() * 1000
                min_delay_wait = max(0, policy.min_delay_ms - delta_ms)

            next_allowed_wait = 0
            if next_allowed:
                next_allowed_wait = max(0, (next_allowed - now).total_seconds() * 1000)

            wait_ms = max(min_delay_wait, next_allowed_wait)

            if policy.crawled_today >= policy.daily_limit:
                next_allowed_time = datetime.combine(
                    now.date() + timedelta(days=1),
                    datetime.min.time(),
                    tzinfo=timezone.utc,
                )
                wait_ms = max(wait_ms, (next_allowed_time - now).total_seconds() * 1000)
                policy.next_allowed_at = next_allowed_time
            elif wait_ms > 0:
                policy.next_allowed_at = now + timedelta(milliseconds=wait_ms)
            else:
                policy.next_allowed_at = None

            if wait_ms <= 0:
                policy.last_crawled_at = now
                policy.crawled_today = (policy.crawled_today or 0) + 1

            await policy.save(using_db=conn)

        if wait_ms > 0:
            await asyncio.sleep(wait_ms / 1000)

    # --------------------------
    #  HTTP fetch with metrics
    # --------------------------
    async def _fetch(self, url: str) -> FetchResult:
        worker_label = str(self.worker_id)
        REQUEST_COUNT.labels(worker=worker_label).inc()

        start = time.perf_counter()
        if self.client is None:
            raise RuntimeError("HTTP client is not initialized")

        try:
            resp = await self.client.get(
                url,
                headers={
                    "User-Agent": self.user_agent,
                    "Accept": (
                        "text/html,application/xhtml+xml,application/xml;q=0.9,"
                        "*/*;q=0.8"
                    ),
                    "Accept-Language": "fa-IR,fa;q=0.9,en-US;q=0.8,en;q=0.7",
                },
            )

            content_type = (resp.headers.get("Content-Type") or "").lower()
            body = resp.content or b""

            if len(body) > MAX_DOWNLOAD_BYTES:
                SKIPPED_LARGE_BODIES.labels(worker=worker_label).inc()
                return FetchResult(
                    status_code=resp.status_code,
                    content="",
                    content_type=content_type,
                    skipped=True,
                    skip_reason="body_too_large",
                )

            if "text/html" not in content_type:
                SKIPPED_NON_HTML.labels(worker=worker_label).inc()
                return FetchResult(
                    status_code=resp.status_code,
                    content="",
                    content_type=content_type,
                    skipped=True,
                    skip_reason="non_html_content",
                )

            return FetchResult(
                status_code=resp.status_code,
                content=resp.text or "",
                content_type=content_type,
                skipped=False,
            )
        finally:
            elapsed = time.perf_counter() - start
            REQUEST_LATENCY.labels(worker=worker_label).observe(elapsed)

    # --------------------------
    #  Main processing
    # --------------------------
    async def process_url(self, task: FrontierTask) -> None:
        worker_label = str(self.worker_id)
        url = task.url

        if "fa.wikipedia.org" in url:
            parsed = urlparse(url)
            if not parsed.path.startswith("/wiki/"):
                logger.info(
                    f"[{self.name}] Skipping non-article Wikipedia URL: {url}"
                )
                await self.queue.mark_done(task.id, None)
                return

        status_code: Optional[int] = None

        try:

            if self.robots_handler is not None:
                allowed = await self.robots_handler.is_allowed(url)
                if not allowed:
                    ROBOTS_SKIPPED.labels(worker=worker_label).inc()
                    logger.info(
                        f"[{self.name}] Skipping disallowed by robots.txt for agent {self.user_agent}: {url}"
                    )
                    await self.queue.mark_done(task.id, None)
                    return

            await self._respect_domain_policy(url)

            fetch_result = await self._fetch(url)

            status_code = fetch_result.status_code

            if fetch_result.skipped:
                await self.queue.mark_done(task.id, status_code)
                WORKER_PROCESSED.labels(worker_id=worker_label).inc()
                logger.info(
                    f"[{self.name}] Skipped {url} ({fetch_result.skip_reason or 'unknown reason'})"
                )
                return

            html = fetch_result.content or ""

            if status_code in {404, 410}:
                await self.queue.mark_done(task.id, status_code)
                WORKER_PROCESSED.labels(worker_id=worker_label).inc()
                SKIPPED_LINKS.labels(reason="http_status").inc()
                logger.info(
                    f"[{self.name}] Skipping {url} due to HTTP status {status_code}"
                )
                return

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

            if not html.strip():
                await self.queue.mark_done(task.id, status_code)
                WORKER_PROCESSED.labels(worker_id=worker_label).inc()
                SKIPPED_LINKS.labels(reason="empty_body").inc()
                logger.info(f"[{self.name}] Empty body returned for {url}")
                return

            if status_code >= 400:
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
            base_domain = get_domain(url)

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

            filtered_links = []
            for link in unique_links:
                if not is_valid_link(base_domain, link):
                    SKIPPED_LINKS.labels(reason="invalid_link").inc()
                    continue
                filtered_links.append(link)

            link_count = len(filtered_links)
            max_links_to_queue = (
                MAX_LINKS_HEAVY_PAGE if heavy_page else MAX_LINKS_PER_PAGE
            )

            if link_count > 0:
                # به‌روزرسانی link_count در متادیتا
                meta.link_count = link_count
                await meta.save()

                # ذخیره OutboundLink و صف کردن لینک‌های جدید
                links_to_queue = filtered_links[:max_links_to_queue]
                skipped_due_to_depth = 0
                next_depth = task.depth + 1
                for link in links_to_queue:
                    parsed = urlparse(link)
                    is_internal = parsed.netloc.lower() == base_domain

                    await OutboundLink.create(
                        source_page=page,
                        target_url=link,
                        is_internal=is_internal,
                    )

                    if (
                        self.queue.max_depth is not None
                        and next_depth > self.queue.max_depth
                    ):
                        skipped_due_to_depth += 1
                        SKIPPED_LINKS.labels(reason="max_depth").inc()
                        continue

                    await self.queue.enqueue_url(
                        link,
                        source_id=task.source_id,
                        depth=next_depth,
                        priority=task.priority,
                    )

                logger.info(
                    f"[{self.name}] Found {link_count} links from {url} "
                    f"(queued {len(links_to_queue) - skipped_due_to_depth} "
                    f"/ skipped {skipped_due_to_depth} due to depth limit)"
                )

            # Metrics
            CRAWLED_PAGES.labels(worker=worker_label).inc()
            WORKER_PROCESSED.labels(worker_id=worker_label).inc()

            logger.info(
                f"[{self.name}] Crawled: {url} ({html_length} bytes, status={status_code})"
            )

            await self.queue.mark_done(task.id, status_code)

        except Exception as e:
            logger.error(f"[{self.name}] Error processing {url}: {e}")
            WORKER_FAILED.labels(worker_id=worker_label).inc()

            safe_status_code = status_code if status_code is not None else 0

            await CrawlErrorLog.create(
                url=url,
                status_code=safe_status_code,
                error_message=str(e),
                worker_id=self.worker_id,
            )
            await self.queue.mark_failed(
                task.id,
                status_code=safe_status_code,
                error_code=str(e),
            )

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
                self.robots_handler = RobotsHandler(client, self.user_agent)

                while True:
                    if self.queue.has_reached_max_pages():
                        logger.info(
                            f"{self.name} stopping; max_pages={self.queue.max_pages} reached"
                        )
                        break

                    task = await self.queue.dequeue_task()
                    if task is None:
                        if self.queue.has_reached_max_pages():
                            logger.info(
                                f"{self.name} stopping; max_pages={self.queue.max_pages} reached"
                            )
                            break
                        await asyncio.sleep(3)
                        continue

                    logger.debug(f"[{self.name}] Dequeued: {task.url}")
                    await self.process_url(task)
        finally:
            self.client = None
            self.robots_handler = None
            WORKER_ACTIVE.labels(worker_id=worker_label).set(0.0)

