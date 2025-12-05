from datetime import datetime

from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient


class MongoStorageManager:
    def __init__(
        self,
        uri: str,
        db_name: str = "jooyacrawler",
        collection_name: str = "pages",
    ):
        self.uri = uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.client: AsyncIOMotorClient | None = None
        self.collection = None

    async def connect(self) -> None:
        self.client = AsyncIOMotorClient(self.uri)
        db = self.client[self.db_name]
        self.collection = db[self.collection_name]
        logger.info(f"Connected to MongoDB: {self.uri}")

    async def close(self) -> None:
        if self.client is not None:
            self.client.close()
            logger.info("MongoDB client connection closed")
        self.client = None
        self.collection = None

    async def save_page(self, url: str, status_code: int, html: str) -> None:
        if self.collection is None:
            raise RuntimeError("MongoStorageManager is not connected")

        doc = {
            "url": url,
            "status_code": status_code,
            "html": html,
            "length": len(html),
            "fetched_at": datetime.utcnow(),
        }

        await self.collection.update_one({"url": url}, {"$set": doc}, upsert=True)


