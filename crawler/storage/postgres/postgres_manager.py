import asyncpg
from loguru import logger

class PostgresManager:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(dsn=self.dsn)
        logger.info("Connected to PostgreSQL")

    async def get_unscheduled_urls(self, limit=10):
        query = """
        SELECT url FROM crawled_pages
        WHERE status_code IS NULL
        ORDER BY id ASC
        LIMIT $1
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, limit)
        return [r["url"] for r in rows]

    async def mark_as_scheduled(self, url):
        query = """
        UPDATE crawled_pages SET status_code = -1 WHERE url = $1
        """
        async with self.pool.acquire() as conn:
            await conn.execute(query, url)

    async def mark_as_crawled(self, url, status_code):
        query = """
        UPDATE crawled_pages
        SET status_code = $2, fetched_at = NOW()
        WHERE url = $1
        """
        async with self.pool.acquire() as conn:
            await conn.execute(query, url, status_code)
