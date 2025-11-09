import redis.asyncio as aioredis
from loguru import logger

class RedisManager:
    def __init__(self, url: str):
        self.url = url
        self.client = None

    async def connect(self):
        self.client = aioredis.from_url(self.url, decode_responses=True)
        logger.info(f"Connected to Redis: {self.url}")

    async def enqueue_url(self, url: str):
        await self.client.lpush("crawler:queue", url)

    async def dequeue_url(self):
        url = await self.client.rpop("crawler:queue")
        return url

    async def add_to_visited(self, url: str):
        await self.client.sadd("crawler:visited", url)

    async def is_visited(self, url: str) -> bool:
        return await self.client.sismember("crawler:visited", url)
