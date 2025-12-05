from crawler.utils.filters import is_valid_link


def test_is_valid_link_accepts_internal_html_pages():
    url = "https://example.com/articles/intro"
    assert is_valid_link("example.com", url)


def test_is_valid_link_rejects_assets_and_external_domains():
    blocked_asset = "https://example.com/image.jpg"
    external = "https://external.com/page"
    javascript = "javascript:alert('x')"

    assert not is_valid_link("example.com", blocked_asset)
    assert not is_valid_link("example.com", external)
    assert not is_valid_link("example.com", javascript)
