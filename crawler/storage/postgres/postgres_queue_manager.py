import datetime as dt
from typing import Optional
import logging
from urllib.parse import urldefrag

from crawler.storage.models.queue_model import CrawlQueue

logger = logging.getLogger(__name__)

# وضعیت‌ها به صورت رشته
STATUS_PENDING = "pending"
STATUS_PROCESSING = "processing"
STATUS_DONE = "done"
STATUS_ERROR = "error"


def _normalize_url(raw: str) -> Optional[str]:
    """
    نرمال‌سازی و فیلتر اولیه‌ی URL قبل از ورود به صف.
    - حذف فاصله‌ها
    - حذف fragment (#...)
    - حذف اسکیم‌های غیر http/https مثل mailto:, javascript:, tel:, data: و ...
    """
    if not raw:
        return None

    raw = raw.strip()
    if not raw:
        return None

    lower = raw.lower()

    # لینک‌هایی که اصلاً نمی‌خوایم کراول کنیم
    if lower.startswith(
        (
            "mailto:",
            "javascript:",
            "tel:",
            "data:",
            "ws:",
            "wss:",
            "irc:",
            "ftp:",
            "file:",
            "about:",
            "chrome:",
            "edge:",
        )
    ):
        return None

    # لینک‌های بدون پروتکل ولی با //example.com
    if lower.startswith("//"):
        raw = "https:" + raw
        lower = raw.lower()

    # فعلاً فقط http/https
    if not (lower.startswith("http://") or lower.startswith("https://")):
        return None

    # حذف fragment
    url, _frag = urldefrag(raw)
    return url


class PostgresQueueManager:
    async def enqueue_url(self, url: str, priority: int = 0) -> None:
        """
        اضافه کردن URL به صف.
        - URL قبل از ذخیره نرمالایز و فیلتر می‌شود.
        - اگر URL قبلاً موجود باشد، فقط در صورت بالاتر بودن priority به‌روزرسانی می‌شود.
        """
        url = _normalize_url(url)
        if not url:
            return

        obj, created = await CrawlQueue.get_or_create(
            url=url,
            defaults={
                "priority": priority,
                "status": STATUS_PENDING,
            },
        )

        # اگر قبلاً در صف بوده و priority جدید بالاتر است، به‌روزرسانی‌اش کن
        if not created:
            try:
                current_priority = obj.priority or 0
            except AttributeError:
                current_priority = 0

            if priority > current_priority:
                obj.priority = priority
                await obj.save()

        logger.debug(
            "Enqueued URL: %s (priority=%s, created=%s)",
            url,
            priority,
            created,
        )

    async def dequeue_url(self) -> Optional[str]:
        """
        گرفتن یک URL از صف با بالاترین priority.
        """
        # اگر فیلدی مثل scheduled_at نداری، این order_by روی priority و id امنه
        row = (
            await CrawlQueue.filter(status=STATUS_PENDING)
            .order_by("-priority", "id")
            .first()
        )

        if not row:
            return None

        row.status = STATUS_PROCESSING
        row.last_attempt_at = dt.datetime.now(dt.timezone.utc)
        await row.save()

        logger.debug("Dequeued: %s", row.url)
        return row.url

    async def mark_done(self, url: str) -> None:
        """
        علامت زدن URL به عنوان done.
        """
        updated = await CrawlQueue.filter(url=url).update(
            status=STATUS_DONE,
            error_count=0,
        )
        logger.debug("Mark done for %s (updated_rows=%s)", url, updated)

    async def mark_error(self, url: str, max_retries: int = 3) -> None:
        """
        افزایش error_count و در صورت لزوم تغییر status به error.
        """
        row = await CrawlQueue.get_or_none(url=url)
        if not row:
            logger.warning("Tried to mark_error for unknown URL: %s", url)
            return

        # اگر null بود، صفر فرض کن
        row.error_count = (row.error_count or 0) + 1

        if row.error_count >= max_retries:
            row.status = STATUS_ERROR
        else:
            # اجازه بده بعداً دوباره امتحان بشه
            row.status = STATUS_PENDING

        row.last_attempt_at = dt.datetime.now(dt.timezone.utc)
        await row.save()

        logger.debug(
            "Mark error for %s (error_count=%s, status=%s)",
            url,
            row.error_count,
            row.status,
        )

    async def has_pending(self) -> bool:
        """
        آیا آیتم pending داریم؟
        """
        return await CrawlQueue.filter(status=STATUS_PENDING).exists()

    async def count_pending(self) -> int:
        """
        تعداد URLهای pending.
        """
        return await CrawlQueue.filter(status=STATUS_PENDING).count()
