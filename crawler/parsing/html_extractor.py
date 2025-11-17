from __future__ import annotations

from typing import List
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup


def extract_title(html: str) -> str | None:
    try:
        soup = BeautifulSoup(html, "lxml")
        if soup.title and soup.title.string:
            return soup.title.string.strip()
    except Exception:
        pass
    return None


def extract_text(html: str) -> str:
    """
    خروجی: متن خالص (برای ایندکس / hash / تحلیل NLP).
    """
    try:
        soup = BeautifulSoup(html, "lxml")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = soup.get_text(separator=" ", strip=True)
        return " ".join(text.split())
    except Exception:
        return ""


def extract_links(base_url: str, html: str) -> List[str]:
    """
    همه لینک‌های داخلی (همان دامنه) را برمی‌گرداند.
    """
    links = set()

    try:
        soup = BeautifulSoup(html, "lxml")
        base_domain = urlparse(base_url).netloc

        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            if not href:
                continue

            full_url = urljoin(base_url, href)
            parsed = urlparse(full_url)

            if parsed.scheme not in ("http", "https"):
                continue

            # فقط لینک‌های داخلی
            if parsed.netloc and parsed.netloc != base_domain:
                continue

            links.add(full_url)

    except Exception:
        pass

    return list(links)
