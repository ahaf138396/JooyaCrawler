import pytest

from crawler.parsing.html_extractor import extract_title, extract_text, extract_links


def test_extract_title_handles_missing_and_whitespace():
    html = "<html><head><title>  Sample Page  </title></head><body></body></html>"
    assert extract_title(html) == "Sample Page"

    html_no_title = "<html><head></head><body></body></html>"
    assert extract_title(html_no_title) == ""


def test_extract_text_strips_non_content_tags():
    html = (
        "<html><head><script>var x=1;</script><style>.cls{}</style></head>"
        "<body><p>Hello</p><noscript>ignore</noscript></body></html>"
    )
    assert extract_text(html) == "Hello"


@pytest.mark.parametrize(
    "href,expected",
    [
        ("/about", "https://example.com/about"),
        ("../relative", "https://example.com/relative"),
        ("javascript:void(0)", None),
    ],
)
def test_extract_links_normalizes_and_skips_js(href, expected):
    html = f"<html><body><a href='{href}'>Link</a></body></html>"
    links = extract_links("https://example.com/base/", html)

    if expected is None:
        assert links == []
    else:
        assert links == [expected]
