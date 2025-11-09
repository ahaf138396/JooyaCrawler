from urllib.parse import urlparse, urlunparse, urljoin, quote
import re

def normalize_url(base_url: str, link: str) -> str | None:
    """نرمال‌سازی لینک‌های نسبی یا دارای پارامتر"""
    try:
        # کامل کردن لینک نسبی
        url = urljoin(base_url, link.strip())
        parsed = urlparse(url)

        # فقط http و https مجازن
        if parsed.scheme not in ("http", "https"):
            return None

        # حذف پارامترهای session و tracking
        clean_query = re.sub(r"(utm_[^=&]+|sessionid|fbclid|ref)=[^&]*", "", parsed.query)
        clean_query = re.sub(r"&&+", "&", clean_query).strip("&")

        # حذف fragment (#...)
        parsed = parsed._replace(query=clean_query, fragment="")

        # حذف slash اضافه
        path = parsed.path or "/"
        if path != "/" and path.endswith("/"):
            path = path[:-1]

        parsed = parsed._replace(path=path)

        normalized = urlunparse(parsed)
        return normalized

    except Exception:
        return None


def is_same_domain(url1: str, url2: str) -> bool:
    """بررسی اینکه دو لینک در یک دامنه هستند یا نه"""
    p1, p2 = urlparse(url1), urlparse(url2)
    return p1.netloc.lower() == p2.netloc.lower()


def get_domain(url: str) -> str:
    """دریافت دامنه از URL"""
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""
