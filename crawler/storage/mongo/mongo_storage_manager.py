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
        self.metadata_collection = None

    async def connect(self) -> None:
        self.client = AsyncIOMotorClient(self.uri)
        db = self.client[self.db_name]
        self.collection = db[self.collection_name]
        self.metadata_collection = db[f"{self.collection_name}_metadata"]
        logger.info(f"Connected to MongoDB: {self.uri}")

    async def find_page(self, url: str):
        if not self.collection:
            raise RuntimeError("MongoStorageManager is not connected")

        return await self.collection.find_one({"url": url})

    async def save_page(
        self, url: str, status_code: int, html: str, text: str, links: list[str]
    ) -> None:
        if not self.collection:
            raise RuntimeError("MongoStorageManager is not connected")

        doc = {
            "url": url,
            "status_code": status_code,
            "html": html,
            "text": text,
            "links": links,
            "length": len(html),
            "fetched_at": datetime.utcnow(),
        }

        await self.collection.update_one({"url": url}, {"$set": doc}, upsert=True)

    async def save_metadata(
        self,
        url: str,
        html_length: int,
        text_length: int,
        link_count: int,
    ) -> None:
        if not self.metadata_collection:
            raise RuntimeError("MongoStorageManager is not connected")

        doc = {
            "url": url,
            "html_length": html_length,
            "text_length": text_length,
            "link_count": link_count,
            "updated_at": datetime.utcnow(),
        }

        await self.metadata_collection.update_one(
            {"url": url}, {"$set": doc}, upsert=True
        )
