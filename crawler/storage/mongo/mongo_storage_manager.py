from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from loguru import logger

class MongoStorageManager:
    def __init__(self, mongo_url: str, db_name: str = "jooyacrawler"):
        self.mongo_url = mongo_url
        self.db_name = db_name
        self.client = None
        self.db = None

    async def connect(self):
        """Connect to MongoDB"""
        self.client = AsyncIOMotorClient(self.mongo_url)
        self.db = self.client[self.db_name]
        logger.info(f"Connected to MongoDB: {self.mongo_url}")

    async def save_page(self, url: str, status_code: int, html: str):
        """Save crawled page content"""
        doc = {
            "url": url,
            "status_code": status_code,
            "content_length": len(html),
            "html": html,
            "timestamp": datetime.utcnow(),
        }
        await self.db.pages.insert_one(doc)
        logger.debug(f"Saved page: {url} ({len(html)} bytes)")