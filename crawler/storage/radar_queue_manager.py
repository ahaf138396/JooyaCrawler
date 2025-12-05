from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterable, Optional

import asyncpg
from loguru import logger

from crawler.utils.db_utils import to_postgres_dsn
from crawler.utils.env_loader import load_environment


STATUS_SCHEDULED = "SCHEDULED"
STATUS_IN_PROGRESS = "IN_PROGRESS"
STATUS_DONE = "DONE"
STATUS_FAILED = "FAILED"


@dataclass
class FrontierTask:
    id: int
    url: str
    source_id: int
    depth: int
    priority: int


class RadarQueueManager:
    """Interact with the Radar frontier tables via asyncpg."""

    def __init__(self, *, max_depth: Optional[int] = None, max_pages: Optional[int] = None) -> None:
        load_environment()
        url = os.getenv("RADAR_DATABASE_URL") or os.getenv("DATABASE_URL")
        if not url:
            user = os.getenv("POSTGRES_USER", "jooya")
            password = os.getenv("POSTGRES_PASSWORD", "postgres")
            host = os.getenv("POSTGRES_HOST", "postgres")
            port = os.getenv("POSTGRES_PORT", "5432")
            db = os.getenv("POSTGRES_DB", "jooyacrawlerdb")
            url = f"postgresql://{user}:{password}@{host}:{port}/{db}"

        self.database_url = to_postgres_dsn(url)
        self.pool: Optional[asyncpg.Pool] = None
        raw_max_depth = os.getenv("MAX_DEPTH")
        try:
            self.max_depth: Optional[int] = max_depth if max_depth is not None else (
                int(raw_max_depth) if raw_max_depth else None
            )
        except ValueError:
            logger.warning(
                f"Invalid MAX_DEPTH value '{raw_max_depth}', disabling depth limit."
            )
            self.max_depth = None

        raw_max_pages = os.getenv("MAX_PAGES")
        self.max_pages: Optional[int]
        try:
            if max_pages is not None:
                self.max_pages = max_pages
            elif raw_max_pages:
                self.max_pages = int(raw_max_pages)
            else:
                self.max_pages = None
        except ValueError:
            logger.warning(
                f"Invalid MAX_PAGES value '{raw_max_pages}', disabling page limit."
            )
            self.max_pages = None

        self.crawled_count = 0

    async def connect(self) -> None:
        if self.pool is None:
            try:
                self.pool = await asyncpg.create_pool(
                    self.database_url, min_size=1, max_size=10
                )
                logger.info("Connected to Radar frontier database")

                if self.max_pages is not None:
                    async with self.pool.acquire() as conn:
                        self.crawled_count = await conn.fetchval(
                            "SELECT count(*) FROM urls_frontier WHERE status = $1",
                            STATUS_DONE,
                        )
            except Exception:
                logger.exception("Failed to connect to Radar frontier database")
                if self.pool:
                    await self.pool.close()
                self.pool = None

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def count_scheduled(self) -> int:
        if not self.pool:
            return 0
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                """
                SELECT count(*) FROM urls_frontier
                WHERE status = $1 AND (scheduled_for IS NULL OR scheduled_for <= NOW())
                """,
                STATUS_SCHEDULED,
            )

    async def dequeue_task(self) -> Optional[FrontierTask]:
        if not self.pool:
            return None
        if self.has_reached_max_pages():
            return None
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE urls_frontier
                SET scheduled_for = NOW() + INTERVAL '1800 seconds'
                WHERE status = $1 AND scheduled_for > NOW() + INTERVAL '1800 seconds'
                """,
                STATUS_SCHEDULED,
            )
            row = await conn.fetchrow(
                """
                WITH next_task AS (
                    SELECT id
                    FROM urls_frontier
                    WHERE status = $1 AND (scheduled_for IS NULL OR scheduled_for <= NOW())
                    ORDER BY priority DESC, id ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE urls_frontier AS f
                SET status = $2, updated_at = NOW()
                FROM next_task
                WHERE f.id = next_task.id
                RETURNING f.id, f.url, f.source_id, f.depth, f.priority;
                """,
                STATUS_SCHEDULED,
                STATUS_IN_PROGRESS,
            )

        if not row:
            return None
        return FrontierTask(
            id=row["id"],
            url=row["url"],
            source_id=row["source_id"],
            depth=row["depth"],
            priority=row["priority"],
        )

    async def mark_done(self, task_id: int, status_code: Optional[int]) -> None:
        if not self.pool:
            return
        async with self.pool.acquire() as conn:
            previous_status = await conn.fetchval(
                """
                WITH previous AS (
                    SELECT id, status AS previous_status
                    FROM urls_frontier
                    WHERE id = $3
                    FOR UPDATE
                )
                UPDATE urls_frontier AS f
                SET status = $1,
                    fail_count = 0,
                    last_http_status = $2,
                    updated_at = NOW()
                FROM previous
                WHERE f.id = previous.id
                RETURNING previous.previous_status
                """,
                STATUS_DONE,
                status_code,
                task_id,
            )

            if self.max_pages is not None and previous_status != STATUS_DONE:
                self.crawled_count += 1

    async def mark_failed(
        self,
        task_id: int,
        *,
        status_code: Optional[int] = None,
        error_code: Optional[str] = None,
        error_category: Optional[str] = None,
    ) -> None:
        if not self.pool:
            return
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                WITH current AS (
                    SELECT id, COALESCE(fail_count, 0) AS fail_count
                    FROM urls_frontier
                    WHERE id = $5
                    FOR UPDATE
                )
                , computed AS (
                    SELECT id,
                        fail_count,
                        LEAST(1800, 30 * POWER(2, fail_count + 1)) AS delay_secs
                    FROM current
                )
                UPDATE urls_frontier AS f
                SET status = $1,
                    fail_count = computed.fail_count + 1,
                    last_http_status = $2,
                    last_error_code = $3,
                    error_category = $4,
                    scheduled_for = NOW() + make_interval(secs => computed.delay_secs),
                    last_scheduled_at = NOW(),
                    updated_at = NOW()
                FROM computed
                WHERE f.id = computed.id
                """,
                STATUS_SCHEDULED,
                status_code,
                error_code,
                error_category,
                task_id,
            )

    async def enqueue_url(
        self,
        url: str,
        *,
        source_id: int,
        depth: int,
        priority: int = 0,
        force_recrawl: bool = False,
    ) -> None:
        if not self.pool:
            return
        if self.max_depth is not None and depth > self.max_depth:
            logger.info(
                "Skipping enqueue for {url} at depth {depth} (max_depth={max_depth})",
                url=url,
                depth=depth,
                max_depth=self.max_depth,
            )
            return
        if self.has_reached_max_pages():
            logger.info(
                "Skipping enqueue for {url}; max_pages limit reached ({max_pages})",
                url=url,
                max_pages=self.max_pages,
            )
            return
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO urls_frontier (url, source_id, depth, priority, status, scheduled_for, last_scheduled_at)
                VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
                ON CONFLICT (url, source_id)
                DO UPDATE SET
                    depth = LEAST(COALESCE(urls_frontier.depth, EXCLUDED.depth), EXCLUDED.depth),
                    source_id = EXCLUDED.source_id,
                    priority = GREATEST(COALESCE(urls_frontier.priority, EXCLUDED.priority), EXCLUDED.priority),
                    status = CASE WHEN urls_frontier.status = $6 AND NOT $7 THEN urls_frontier.status ELSE EXCLUDED.status END,
                    scheduled_for = CASE WHEN urls_frontier.status = $6 AND NOT $7 THEN urls_frontier.scheduled_for ELSE EXCLUDED.scheduled_for END,
                    last_scheduled_at = CASE WHEN urls_frontier.status = $6 AND NOT $7 THEN urls_frontier.last_scheduled_at ELSE EXCLUDED.last_scheduled_at END,
                    updated_at = NOW()
                """,
                url,
                source_id,
                depth,
                priority,
                STATUS_SCHEDULED,
                STATUS_DONE,
                force_recrawl,
            )

    async def enqueue_many(
        self,
        urls: Iterable[str],
        *,
        source_id: int,
        depth: int,
        priority: int = 0,
        force_recrawl: bool = False,
    ) -> None:
        if not self.pool:
            return
        if self.max_depth is not None and depth > self.max_depth:
            logger.info(
                "Skipping bulk enqueue at depth {depth} (max_depth={max_depth})",
                depth=depth,
                max_depth=self.max_depth,
            )
            return
        if self.has_reached_max_pages():
            logger.info(
                "Skipping bulk enqueue; max_pages limit reached ({max_pages})",
                max_pages=self.max_pages,
            )
            return

        payload = [u for u in urls if u]
        if not payload:
            return

        sql = """
            INSERT INTO urls_frontier (url, source_id, depth, priority, status, scheduled_for, last_scheduled_at)
            VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
            ON CONFLICT (url, source_id)
            DO UPDATE SET
                depth = LEAST(COALESCE(urls_frontier.depth, EXCLUDED.depth), EXCLUDED.depth),
                source_id = EXCLUDED.source_id,
                priority = GREATEST(COALESCE(urls_frontier.priority, EXCLUDED.priority), EXCLUDED.priority),
                status = CASE WHEN urls_frontier.status = $6 AND NOT $7 THEN urls_frontier.status ELSE EXCLUDED.status END,
                scheduled_for = CASE WHEN urls_frontier.status = $6 AND NOT $7 THEN urls_frontier.scheduled_for ELSE EXCLUDED.scheduled_for END,
                last_scheduled_at = CASE WHEN urls_frontier.status = $6 AND NOT $7 THEN urls_frontier.last_scheduled_at ELSE EXCLUDED.last_scheduled_at END,
                updated_at = NOW()
        """

        async with self.pool.acquire() as conn:
            await conn.executemany(
                sql,
                [
                    (
                        url,
                        source_id,
                        depth,
                        priority,
                        STATUS_SCHEDULED,
                        STATUS_DONE,
                        force_recrawl,
                    )
                    for url in payload
                ],
            )

    def has_reached_max_pages(self) -> bool:
        return self.max_pages is not None and self.crawled_count >= self.max_pages
