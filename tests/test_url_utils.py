from crawler.utils.url_utils import get_domain, is_same_domain, normalize_url


def test_normalize_url_resolves_relative_and_cleans_tracking():
    normalized = normalize_url(
        "https://example.com/base/",
        "/path/?utm_source=test&utm_medium=ad#section",
    )
    assert normalized == "https://example.com/path"


def test_normalize_url_rejects_non_http_schemes():
    assert normalize_url("https://example.com", "ftp://example.com/file") is None


def test_is_same_domain_and_get_domain():
    assert is_same_domain("https://example.com/page", "https://example.com/other")
    assert not is_same_domain("https://example.com", "https://another.com")
    assert get_domain("not-a-url") == ""
    assert get_domain("https://Sub.Example.com") == "sub.example.com"
