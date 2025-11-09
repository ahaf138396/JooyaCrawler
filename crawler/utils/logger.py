from loguru import logger
import os

def setup_logger(log_level="INFO", log_path="/data/logs/crawler.log"):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logger.remove()  # حذف لاگر پیش‌فرض
    logger.add(
        log_path,
        rotation="10 MB",
        retention="7 days",
        level=log_level,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
    )
    logger.add(
        lambda msg: print(msg, end=""),
        colorize=True,
        level=log_level
    )
    return logger
