from datetime import datetime
from typing import Optional

from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConfigurationError


class MongoStorageManager:
    def __init__(
        self,
        uri: str,
        db_name: str | None = None,
        collection_name: str = "pages",
        max_html_bytes: Optional[int] = None,
    ):
        self.uri = uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.max_html_bytes = max_html_bytes
        self.client: AsyncIOMotorClient | None = None
        self.collection = None

    async def connect(self) -> None:
        self.client = AsyncIOMotorClient(self.uri)
        db = None

        if self.db_name:
            db = self.client[self.db_name]
        else:
            try:
                db = self.client.get_default_database()
            except ConfigurationError:
                db = None

        if db is None:
            # fallback برای زمانی که در URI نام دیتابیس مشخص نشده
            db = self.client["jooyacrawler"]

        self.db_name = db.name
        self.collection = db[self.collection_name]
        logger.info(f"Connected to MongoDB: {self.uri} (db={self.db_name})")

    async def close(self) -> None:
        if self.client is not None:
            self.client.close()
            logger.info("MongoDB client connection closed")
        self.client = None
        self.collection = None

    async def save_page(self, url: str, status_code: int, html: str) -> None:
        if self.collection is None:
            raise RuntimeError("MongoStorageManager is not connected")

        truncated_html = html
        if self.max_html_bytes is not None and len(truncated_html) > self.max_html_bytes:
            truncated_html = truncated_html[: self.max_html_bytes]

        doc = {
            "url": url,
            "status_code": status_code,
            "html": truncated_html,
            "length": len(truncated_html),
            "fetched_at": datetime.utcnow(),
        }

        await self.collection.update_one({"url": url}, {"$set": doc}, upsert=True)


