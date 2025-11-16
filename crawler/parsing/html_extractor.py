from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse


def normalize_url(url: str) -> str:
    """
    نرمالایز کردن آدرس:
    - حذف / اضافه
    - حذف query string برای کم شدن تکراری‌ها
    """
    parsed = urlparse(url)
    clean_path = parsed.path.rstrip("/") or "/"
    clean = parsed._replace(path=clean_path, query="")
    return urlunparse(clean)


def extract_title(html: str) -> str | None:
    try:
        soup = BeautifulSoup(html, "html.parser")
        title_tag = soup.find("title")
        return title_tag.get_text(strip=True) if title_tag else None
    except Exception:
        return None


def extract_links(base_url: str, html: str) -> list[str]:
    """
    استخراج لینک‌ های داخلی (همین دامنه) – فقط http/https – نرمال شده.
    """
    links = set()
    try:
        soup = BeautifulSoup(html, "html.parser")
        base_domain = urlparse(base_url).netloc.lower()

        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            full = urljoin(base_url, href)
            parsed = urlparse(full)

            if parsed.scheme not in ("http", "https"):
                continue

            if base_domain not in parsed.netloc.lower():
                continue

            links.add(normalize_url(full))
    except Exception:
        pass

    return list(links)