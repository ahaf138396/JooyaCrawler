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
    SKIPPED_NON_HTML,
    SKIPPED_LARGE_BODIES,
)


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

    # --------------------------
    #  Domain crawl policy
    # --------------------------
    async def _respect_domain_policy(self, url: str) -> None:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        # Initial read without locking to calculate any wait time before acquiring locks
        while True:
            initial_policy = await DomainCrawlPolicy.filter(domain=domain).first()
            now = datetime.now(timezone.utc)

            wait_ms = 0
            if initial_policy:
                last_crawled = initial_policy.last_crawled_at
                next_allowed = initial_policy.next_allowed_at

                if next_allowed and next_allowed.tzinfo is None:
                    next_allowed = next_allowed.replace(tzinfo=timezone.utc)
                if last_crawled and last_crawled.tzinfo is None:
                    last_crawled = last_crawled.replace(tzinfo=timezone.utc)

                if next_allowed:
                    wait_ms = max(wait_ms, (next_allowed - now).total_seconds() * 1000)
                if last_crawled:
                    delta_ms = (now - last_crawled).total_seconds() * 1000
                    wait_ms = max(wait_ms, initial_policy.min_delay_ms - delta_ms)

            if wait_ms > 0:
                await asyncio.sleep(wait_ms / 1000)

            async with in_transaction() as conn:
                policy = (
                    await DomainCrawlPolicy.filter(domain=domain)
                    .using_db(conn)
                    .select_for_update()
                    .first()
                )

                now = datetime.now(timezone.utc)

                if policy is None:
                    policy = await DomainCrawlPolicy.using_db(conn).create(
                        domain=domain,
                        min_delay_ms=1000,
                        last_crawled_at=None,
                        next_allowed_at=None,
                        crawled_today=0,
                    )

                last_crawled = policy.last_crawled_at
                next_allowed = policy.next_allowed_at
                if last_crawled and last_crawled.tzinfo is None:
                    last_crawled = last_crawled.replace(tzinfo=timezone.utc)
                if next_allowed and next_allowed.tzinfo is None:
                    next_allowed = next_allowed.replace(tzinfo=timezone.utc)

                wait_ms_locked = 0
                if next_allowed and now < next_allowed:
                    wait_ms_locked = max(
                        wait_ms_locked, (next_allowed - now).total_seconds() * 1000
                    )

                if last_crawled is not None:
                    delta_ms = (now - last_crawled).total_seconds() * 1000
                    wait_ms_locked = max(wait_ms_locked, policy.min_delay_ms - delta_ms)
                    if last_crawled.date() != now.date():
                        policy.crawled_today = 0
                else:
                    policy.crawled_today = 0

                if policy.crawled_today >= policy.daily_limit:
                    next_allowed_time = datetime.combine(
                        now.date() + timedelta(days=1),
                        datetime.min.time(),
                        tzinfo=timezone.utc,
                    )
                    policy.next_allowed_at = next_allowed_time
                    wait_ms_locked = max(
                        wait_ms_locked,
                        (next_allowed_time - now).total_seconds() * 1000,
                    )
                    await policy.save(using_db=conn)
                elif wait_ms_locked > 0:
                    # Persist state changes (e.g., daily reset) before waiting
                    policy.next_allowed_at = None
                    await policy.save(using_db=conn)
                else:
                    policy.last_crawled_at = now
                    policy.next_allowed_at = None
                    policy.crawled_today = (policy.crawled_today or 0) + 1
                    await policy.save(using_db=conn)
                    return

            if wait_ms_locked > 0:
                await asyncio.sleep(wait_ms_locked / 1000)
            else:
                return

    # --------------------------
    #  HTTP fetch with metrics
    # --------------------------
    async def _fetch(self, url: str) -> FetchResult:
        worker_label = str(self.worker_id)
        REQUEST_COUNT.labels(worker=worker_label).inc()

        start = time.perf_counter()
        if self.client is None:
            raise RuntimeError("HTTP client is not initialized")

        headers = {
            "User-Agent": "JooyaCrawler/0.1 (+https://example.com)",
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9," "*/*;q=0.8"
            ),
            "Accept-Language": "fa-IR,fa;q=0.9,en-US;q=0.8,en;q=0.7",
        }

        try:
            async with self.client.stream("GET", url, headers=headers) as resp:
                content_type_header = (resp.headers.get("content-type") or "").lower()
                content_type = content_type_header.split(";")[0].strip()

                content_length_header = resp.headers.get("content-length")
                content_length = None
                if content_length_header:
                    try:
                        content_length = int(content_length_header)
                    except ValueError:
                        logger.debug(
                            f"[{self.name}] Invalid content-length header: {content_length_header}"
                        )

                if content_length is not None and content_length > MAX_DOWNLOAD_BYTES:
                    SKIPPED_LARGE_BODIES.labels(worker=worker_label).inc()
                    logger.warning(
                        f"[{self.name}] Skipping {url} due to content-length={content_length} "
                        f"> limit {MAX_DOWNLOAD_BYTES}"
                    )
                    return FetchResult(
                        status_code=resp.status_code,
                        content="",
                        content_type=content_type,
                        skipped=True,
                        skip_reason="body-too-large",
                    )

                if content_type and content_type not in (
                    "text/html",
                    "application/xhtml+xml",
                ):
                    SKIPPED_NON_HTML.labels(worker=worker_label).inc()
                    logger.info(
                        f"[{self.name}] Skipping {url} due to non-HTML content-type: {content_type}"
                    )
                    return FetchResult(
                        status_code=resp.status_code,
                        content="",
                        content_type=content_type,
                        skipped=True,
                        skip_reason="non-html-content-type",
                    )

                collected: list[bytes] = []
                total_bytes = 0
                async for chunk in resp.aiter_bytes():
                    if not chunk:
                        continue
                    total_bytes += len(chunk)
                    if total_bytes > MAX_DOWNLOAD_BYTES:
                        SKIPPED_LARGE_BODIES.labels(worker=worker_label).inc()
                        logger.warning(
                            f"[{self.name}] Aborting download for {url}; body exceeded {MAX_DOWNLOAD_BYTES} bytes"
                        )
                        return FetchResult(
                            status_code=resp.status_code,
                            content="",
                            content_type=content_type,
                            skipped=True,
                            skip_reason="body-too-large",
                        )
                    collected.append(chunk)

                encoding = resp.encoding or "utf-8"
                html = b"".join(collected).decode(encoding, errors="ignore")

                return FetchResult(
                    status_code=resp.status_code,
                    content=html,
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

            await self._respect_domain_policy(url)

            fetch_result = await self._fetch(url)

            status_code = fetch_result.status_code

            if fetch_result.skipped:
                await self.queue.mark_done(task.id, status_code)
                logger.info(
                    f"[{self.name}] Skipped {url} ({fetch_result.skip_reason or 'unknown reason'})"
                )
                return

            html = fetch_result.content or ""

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

                while True:
                    task = await self.queue.dequeue_task()
                    if task is None:
                        await asyncio.sleep(3)
                        continue

                    logger.debug(f"[{self.name}] Dequeued: {task.url}")
                    await self.process_url(task)
        finally:
            self.client = None
            WORKER_ACTIVE.labels(worker_id=worker_label).set(0.0)

