import re
from urllib.parse import urlparse
from crawler.utils.url_utils import get_domain

# پسوندهایی که نباید خزیده بشن
BLOCKED_EXTENSIONS = (
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".mp4", ".mp3", ".pdf",
    ".zip", ".rar", ".exe", ".apk", ".iso", ".tar", ".gz", ".7z", ".css", ".js"
)

def is_valid_link(base_domain: str, url: str) -> bool:
    """بررسی اینکه URL برای خزش مناسب هست یا نه"""
    parsed = urlparse(url)
    if not parsed.scheme.startswith("http"):
        return False

    # فایل‌ها و منابع استاتیک
    combined_path = parsed.path
    if parsed.query:
        combined_path = f"{combined_path}?{parsed.query}"

    combined_path = combined_path.lower()

    if any(re.search(re.escape(ext) + r"(?:$|[?#&])", combined_path) for ext in BLOCKED_EXTENSIONS):
        return False

    # لینک‌های خالی یا بی‌دامنه
    if not parsed.netloc:
        return False

    # لینک‌هایی با کاراکترهای مشکوک (مثل جاوااسکریپت)
    if re.match(r"^(javascript:|mailto:|tel:)", parsed.geturl(), re.I):
        return False

    # فقط لینک‌های داخلی (دامنه مشابه)
    # توجه: زیر دامنه‌ها عمداً به عنوان داخلی محسوب نمی‌شوند
    if get_domain(url) != base_domain:
        return False

    return True
