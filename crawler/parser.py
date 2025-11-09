from selectolax.parser import HTMLParser
from crawler.utils.url_utils import normalize_url, get_domain
from crawler.utils.filters import is_valid_link

class Parser:
    def extract_links(self, base_url: str, html: str):
        """استخراج لینک‌های تمیز و معتبر از صفحه"""
        tree = HTMLParser(html)
        base_domain = get_domain(base_url)
        links = set()

        for node in tree.css("a[href]"):
            href = node.attributes.get("href")
            normalized = normalize_url(base_url, href)
            if normalized and is_valid_link(base_domain, normalized):
                links.add(normalized)

        return list(links)