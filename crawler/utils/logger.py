from loguru import logger
import os

_logger_initialized = False
_sink_ids: list[int] = []


def setup_logger(log_level: str = "INFO", log_path: str = "/data/logs/crawler.log", worker_id: str | None = None):
    global _logger_initialized, _sink_ids

    resolved_worker_id = worker_id or os.getenv("WORKER_ID") or str(os.getpid())

    if not _logger_initialized:
        os.makedirs(os.path.dirname(log_path), exist_ok=True)

        try:
            logger.remove()  # حذف لاگر پیش‌فرض
        except Exception:
            # در صورت حذف شدن قبلی، از خطا جلوگیری می‌کنیم
            pass

        logger.configure(extra={"worker_id": resolved_worker_id})

        file_sink = logger.add(
            log_path,
            rotation="10 MB",
            retention="7 days",
            level=log_level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | worker={extra[worker_id]} | {message}",
        )
        console_sink = logger.add(
            lambda msg: print(msg, end=""),
            colorize=True,
            level=log_level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | worker={extra[worker_id]} | {message}",
        )

        _sink_ids = [file_sink, console_sink]
        _logger_initialized = True

    bound_logger = logger.bind(worker_id=resolved_worker_id)
    return bound_logger
