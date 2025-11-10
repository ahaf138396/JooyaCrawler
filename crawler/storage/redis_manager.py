import asyncio
import logging
import redis.asyncio as redisi
from redis.asyncio.client import Redis
from typing import Optional


logger = logging.getLogger(__name__)


class RedisManager:
    """
    RedisManager handles async connections, queuing, and retrieval of crawl URLs.
    Supports secure authentication, retry logic, and graceful reconnection.
    """

    def __init__(self, redis_url: str, max_retries: int = 3, retry_delay: float = 2.0):
        self.redis_url = redis_url
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.client: Optional[Redis] = None

    async def connect(self) -> None:
        """Initialize async Redis connection with retry logic."""
        for attempt in range(1, self.max_retries + 1):
            try:
                self.client = await redisi.from_url(
                    self.redis_url,
                    decode_responses=True,
                    socket_timeout=5,
                    health_check_interval=30,
                )
                pong = await self.client.ping()
                if pong:
                    logger.info(f"Connected to Redis: {self.redis_url}")
                    return
            except Exception as e:
                logger.warning(f"[Redis] Connection attempt {attempt} failed: {e}")
                await asyncio.sleep(self.retry_delay)

        raise ConnectionError(f"[Redis] Could not connect after {self.max_retries} retries.")

    async def enqueue_url(self, url: str) -> None:
        """Push a new URL into the crawl queue."""
        if not self.client:
            raise RuntimeError("Redis client is not connected.")
        await self.client.lpush("crawler:queue", url)
        logger.debug(f"Enqueued URL: {url}")

    async def dequeue_url(self) -> Optional[str]:
        """Retrieve and remove one URL from the crawl queue."""
        if not self.client:
            raise RuntimeError("Redis client is not connected.")
        url = await self.client.rpop("crawler:queue")
        if url:
            logger.debug(f"Dequeued URL: {url}")
        return url

    async def cache_page(self, key: str, content: str, expire_seconds: int = 3600) -> None:
        """Cache a crawled page temporarily."""
        if not self.client:
            raise RuntimeError("Redis client is not connected.")
        await self.client.setex(f"page:{key}", expire_seconds, content)
        logger.debug(f"Cached page under key {key}")

    async def get_cached_page(self, key: str) -> Optional[str]:
        """Retrieve a cached page."""
        if not self.client:
            raise RuntimeError("Redis client is not connected.")
        return await self.client.get(f"page:{key}")

    async def disconnect(self) -> None:
        """Gracefully close the Redis connection."""
        if self.client:
            await self.client.close()
            logger.info("Redis connection closed.")
