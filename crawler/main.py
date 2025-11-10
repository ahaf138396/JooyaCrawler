import asyncio
from crawler.storage.postgres_init import init_postgres
from crawler.storage.postgres_queue_manager import PostgresQueueManager
from crawler.scheduler import Scheduler
from crawler.worker import Worker

async def main():
    await init_postgres()
    queue = PostgresQueueManager()
    scheduler = Scheduler(queue)

    # ۳ Worker به صورت همزمان
    workers = [Worker(queue, i).run() for i in range(3)]

    # اجرای همزمان Scheduler و Workers
    await asyncio.gather(scheduler.run(), *workers)

if __name__ == "__main__":
    asyncio.run(main())
