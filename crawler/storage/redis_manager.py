try:
    import redis.asyncio as redis
except ImportError:
    import redis as redis  # fallback for legacy

class RedisManager:
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.client = None

    async def connect(self):
        # redis.asyncio.from_url در نسخه‌های 4 به بالا وجود دارد
        if hasattr(redis, "asyncio"):
            self.client = await redis.from_url(self.redis_url, decode_responses=True)
            print(f"Connected to Redis: {self.redis_url}")
        else:
            # نسخه‌های قدیمی اصلاً async ندارند، باید sync باشند
            raise RuntimeError("Your redis-py version is too old. Please upgrade to >=4.2.0.")



    async def enqueue_url(self, url: str):
        await self.client.lpush("crawler:queue", url)

    async def dequeue_url(self):
        return await self.client.rpop("crawler:queue")

    async def add_to_visited(self, url: str):
        await self.client.sadd("crawler:visited", url)

    async def is_visited(self, url: str) -> bool:
        return await self.client.sismember("crawler:visited", url)
