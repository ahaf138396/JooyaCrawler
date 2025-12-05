from urllib.parse import urlparse, urlunparse, urljoin
import re


def _clean_tracking_params(query: str) -> str:
    clean_query = re.sub(r"(utm_[^=&]+|sessionid|fbclid|ref|gclid)=[^&]*", "", query, flags=re.IGNORECASE)
    clean_query = re.sub(r"&&+", "&", clean_query).strip("&")
    return clean_query


def normalize_url(base_url: str, link: str) -> str | None:
    """نرمال‌سازی لینک‌های نسبی یا دارای پارامتر"""
    try:
        raw_link = link.strip()
        if raw_link.startswith("//"):
            # لینک‌های بدون scheme ولی با دامنه
            base_scheme = urlparse(base_url).scheme or "http"
            raw_link = f"{base_scheme}:{raw_link}"

        url = urljoin(base_url, raw_link)
        parsed = urlparse(url)

        # فقط http و https مجازن
        if parsed.scheme not in ("http", "https"):
            return None

        clean_query = _clean_tracking_params(parsed.query)

        # حذف fragment (#...)
        parsed = parsed._replace(query=clean_query, fragment="")

        # حذف slash اضافه و duplicate ها
        path = parsed.path or "/"
        path = re.sub(r"/{2,}", "/", path)
        if path != "/" and path.endswith("/"):
            path = path[:-1]

        netloc = parsed.netloc.lower()

        parsed = parsed._replace(path=path, netloc=netloc)

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
        netloc = urlparse(url).netloc.lower()
        # حذف پورت برای مقایسه صحیح دامنه
        return netloc.split(":", 1)[0]
    except Exception:
        return ""
