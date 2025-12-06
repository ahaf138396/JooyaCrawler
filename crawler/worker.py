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
    extract_canonical_url,
)
from crawler.storage.mongo.mongo_storage_manager import MongoStorageManager
from crawler.storage.radar_queue_manager import FrontierTask, RadarQueueManager

from crawler.storage.models.page_model import CrawledPage
from crawler.storage.models.outbound_link_model import OutboundLink
from crawler.storage.models.page_metadata_model import PageMetadata
from crawler.storage.models.crawl_error_log_model import CrawlErrorLog
from crawler.storage.models.domain_crawl_policy_model import DomainCrawlPolicy
from tortoise.exceptions import OperationalError as DBError
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
MAX_REDIRECTS = 10


@dataclass
class FetchResult:
    status_code: int
    content: str
    content_type: str
    skipped: bool
    redirect_count: int = 0
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
        self.request_timeout = config.request_timeout
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

            redirect_count = len(resp.history)
            if redirect_count > MAX_REDIRECTS:
                return FetchResult(
                    status_code=resp.status_code,
                    content="",
                    content_type=content_type,
                    skipped=True,
                    redirect_count=redirect_count,
                    skip_reason="redirect_loop",
                )

            if len(body) > MAX_DOWNLOAD_BYTES:
                SKIPPED_LARGE_BODIES.labels(worker=worker_label).inc()
                return FetchResult(
                    status_code=resp.status_code,
                    content="",
                    content_type=content_type,
                    skipped=True,
                    redirect_count=redirect_count,
                    skip_reason="body_too_large",
                )

            if "text/html" not in content_type:
                SKIPPED_NON_HTML.labels(worker=worker_label).inc()
                return FetchResult(
                    status_code=resp.status_code,
                    content="",
                    content_type=content_type,
                    skipped=True,
                    redirect_count=redirect_count,
                    skip_reason="non_html_content",
                )

            return FetchResult(
                status_code=resp.status_code,
                content=resp.text or "",
                content_type=content_type,
                skipped=False,
                redirect_count=redirect_count,
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

        status_code: Optional[int] = None

        try:
            # --------------------------
            # 1) Pre-checks
            # --------------------------
            if "fa.wikipedia.org" in url:
                parsed = urlparse(url)
                if not parsed.path.startswith("/wiki/"):
                    logger.info(
                        f"[{self.name}] Skipping non-article Wikipedia URL: {url}"
                    )
                    await self.queue.mark_done(task.id, None)
                    WORKER_PROCESSED.labels(worker_id=worker_label).inc()
                    return

            if self.robots_handler is not None:
                allowed = await self.robots_handler.is_allowed(url)
                if not allowed:
                    ROBOTS_SKIPPED.labels(worker=worker_label).inc()
                    logger.info(
                        f"[{self.name}] Skipping disallowed by robots.txt for agent {self.user_agent}: {url}"
                    )
                    await self.queue.mark_done(task.id, None)
                    WORKER_PROCESSED.labels(worker_id=worker_label).inc()
                    return

            await self._respect_domain_policy(url)

            # --------------------------
            # 2) Fetch stage
            # --------------------------
            fetch_result = await self._fetch(url)
            status_code = fetch_result.status_code

            if status_code in {404, 410}:
                await self.queue.mark_done(task.id, status_code)
                WORKER_PROCESSED.labels(worker_id=worker_label).inc()
                SKIPPED_LINKS.labels(reason="http_status").inc()
                logger.info(
                    f"[{self.name}] Skipping {url} due to HTTP status {status_code}"
                )
                return

            if fetch_result.skipped:
                await self.queue.mark_done(task.id, status_code)
                WORKER_PROCESSED.labels(worker_id=worker_label).inc()
                logger.info(
                    f"[{self.name}] Skipped {url} ({fetch_result.skip_reason or 'unknown reason'})"
                )
                return

            html = fetch_result.content or ""

            if not html.strip():
                await self.queue.mark_done(task.id, status_code)
                WORKER_PROCESSED.labels(worker_id=worker_label).inc()
                SKIPPED_LINKS.labels(reason="empty_body").inc()
                logger.info(f"[{self.name}] Empty body returned for {url}")
                return

            if status_code >= 400:
                FAILED_REQUESTS.labels(worker=worker_label).inc()
                raise Exception(f"Non-success status code: {status_code}")

            heavy_page = len(html) > MAX_PARSE_BYTES
            parse_source = html if not heavy_page else html[:MAX_PARSE_BYTES]
            if heavy_page:
                logger.debug(
                    f"[{self.name}] Large page detected ({len(html)} bytes); "
                    f"parsing first {MAX_PARSE_BYTES} bytes"
                )

            # --------------------------
            # 3) Parsing stage
            # --------------------------
            canonical_url = extract_canonical_url(url, parse_source) or ""
            base_for_links = canonical_url or url
            title = extract_title(parse_source) or ""
            text = extract_text(parse_source) or ""

            links_raw = extract_links(base_for_links, parse_source) or []
            seen: set[str] = set()
            unique_links = []
            for link in links_raw:
                if link and link not in seen:
                    seen.add(link)
                    unique_links.append(link)

            base_domain = get_domain(base_for_links)
            filtered_links = []
            for link in unique_links:
                if not is_valid_link(base_domain, link):
                    SKIPPED_LINKS.labels(reason="invalid_link").inc()
                    continue
                parsed_link = urlparse(link)
                if parsed_link.netloc.lower() != base_domain:
                    continue
                filtered_links.append(link)

            max_links_to_queue = (
                MAX_LINKS_HEAVY_PAGE if heavy_page else MAX_LINKS_PER_PAGE
            )
            filtered_links = filtered_links[:max_links_to_queue]
            link_count = len(filtered_links)

            # --------------------------
            # 4) Storage stage
            # --------------------------
            await self.mongo.save_page(url, status_code, html)

            html_length = len(html)
            text_length = len(text)
            content_for_hash = text if text else html
            content_hash = hashlib.sha256(
                content_for_hash.encode("utf-8", errors="ignore")
            ).hexdigest()
            content_preview = html[:5000]

            async with in_transaction() as conn:
                page, _ = await CrawledPage.get_or_create(
                    url=url,
                    defaults={
                        "status_code": status_code,
                        "title": title,
                        "content": content_preview,
                    },
                    using_db=conn,
                )

                page.status_code = status_code
                page.title = title
                page.content = content_preview
                await page.save(using_db=conn)

                meta, _ = await PageMetadata.get_or_create(
                    page=page,
                    defaults={
                        "html_length": html_length,
                        "text_length": text_length,
                        "link_count": link_count,
                        "language": None,
                        "content_hash": content_hash,
                        "keywords": None,
                    },
                    using_db=conn,
                )

                meta.html_length = html_length
                meta.text_length = text_length
                meta.link_count = link_count
                meta.content_hash = content_hash
                await meta.save(using_db=conn)

                next_depth = task.depth + 1
                outbound_links = filtered_links
                for link in outbound_links:
                    parsed_link = urlparse(link)
                    is_internal = parsed_link.netloc.lower() == base_domain
                    await OutboundLink.create(
                        source_page=page,
                        target_url=link,
                        is_internal=is_internal,
                        using_db=conn,
                    )

            # --------------------------
            # 5) Link enqueue stage
            # --------------------------
            next_depth = task.depth + 1
            if self.queue.max_depth is not None and next_depth > self.queue.max_depth:
                SKIPPED_LINKS.labels(reason="max_depth").inc()
                logger.info(
                    f"[{self.name}] Depth limit reached; skipping enqueue for {url} links"
                )
                filtered_links = []

            if self.queue.has_reached_max_pages():
                logger.info(
                    f"[{self.name}] Max pages reached; not enqueuing new links from {url}"
                )
                filtered_links = []

            if filtered_links:
                enqueue_fn = getattr(self.queue, "enqueue_many", None)
                if enqueue_fn:
                    await enqueue_fn(
                        filtered_links,
                        source_id=task.source_id,
                        depth=next_depth,
                        priority=task.priority,
                    )
                else:
                    for link in filtered_links:
                        await self.queue.enqueue_url(
                            link,
                            source_id=task.source_id,
                            depth=next_depth,
                            priority=task.priority,
                        )

            # --------------------------
            # 6) Completion stage
            # --------------------------
            await self.queue.mark_done(task.id, status_code)
            WORKER_PROCESSED.labels(worker_id=worker_label).inc()
            CRAWLED_PAGES.labels(worker=worker_label).inc()
            logger.info(
                f"[{self.name}] Crawled: {url} ({html_length} bytes, status={status_code}, links={link_count})"
            )

        except Exception as e:
            logger.error(f"[{self.name}] Error processing {url}: {e}")
            WORKER_FAILED.labels(worker_id=worker_label).inc()

            safe_status_code = status_code if status_code is not None else 0
            error_message = (str(e) or "")[:500]
            error_category = self._categorize_error(e)

            await CrawlErrorLog.create(
                url=url,
                status_code=safe_status_code,
                error_message=error_message,
                worker_id=self.worker_id,
            )
            await self.queue.mark_failed(
                task.id,
                status_code=safe_status_code,
                error_code=error_message,
                error_category=error_category,
            )

    def _categorize_error(self, exc: Exception) -> str:
        if isinstance(exc, (httpx.TimeoutException, asyncio.TimeoutError)):
            return "network_timeout"
        if isinstance(exc, httpx.TransportError):
            return "connection_error"
        if isinstance(exc, DBError):
            return "db_error"
        if isinstance(exc, (ValueError, UnicodeDecodeError, AttributeError)):
            return "parse_error"
        return "unexpected"

    # --------------------------
    #  Worker loop
    # --------------------------
    async def run(self) -> None:
        worker_label = str(self.worker_id)
        WORKER_ACTIVE.labels(worker_id=worker_label).set(1.0)

        logger.info(f"{self.name} started.")

        timeout = httpx.Timeout(timeout=self.request_timeout)

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

