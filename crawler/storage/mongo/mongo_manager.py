from pymongo import MongoClient
from loguru import logger

class MongoManager:
    def __init__(self, url: str):
        self.url = url
        self.client = None
        self.db = None

    async def connect(self):
        self.client = MongoClient(self.url)
        self.db = self.client.get_database()
        logger.info(f"Connected to MongoDB: {self.url}")

    async def save_page(self, url: str, html: str):
        try:
            self.db.pages.update_one(
                {"url": url},
                {"$set": {"html": html}},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Mongo save failed for {url}: {e}")
