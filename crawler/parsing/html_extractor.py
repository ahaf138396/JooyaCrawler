from bs4 import BeautifulSoup
from urllib.parse import urljoin


def extract_title(html: str) -> str:
    """
    Extract <title> safely.
    Always returns a string.
    """
    try:
        soup = BeautifulSoup(html, "lxml")
        if soup.title and soup.title.string:
            return soup.title.string.strip()
        return ""
    except Exception:
        return ""


def extract_text(html: str) -> str:
    """
    Extract visible text.
    Always returns a string (never None).
    """
    try:
        soup = BeautifulSoup(html, "lxml")

        # Remove scripts/styles for cleaner text
        for tag in soup(["script", "style", "noscript"]):
            tag.extract()

        text = soup.get_text(separator=" ", strip=True)
        return text if isinstance(text, str) else ""
    except Exception:
        return ""


def extract_links(base_url: str, html: str) -> list[str]:
    """
    Extract all <a href>, normalized, cleaned.
    Always returns a list[str].
    """
    links = []

    try:
        soup = BeautifulSoup(html, "lxml")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()

            # skip javascript links
            if href.startswith("javascript:"):
                continue

            # absolute-ize
            abs_url = urljoin(base_url, href)

            links.append(abs_url)

        return links
    except Exception:
        return []
