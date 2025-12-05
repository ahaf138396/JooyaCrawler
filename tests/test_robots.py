import pytest

from crawler.utils.robots import RobotsHandler


class MockResponse:
    def __init__(self, status_code: int, text: str = ""):
        self.status_code = status_code
        self.text = text


class MockClient:
    def __init__(self, responses):
        self.responses = responses
        self.calls = 0

    async def get(self, *_args, **_kwargs):
        response = self.responses[min(self.calls, len(self.responses) - 1)]
        self.calls += 1
        return response


@pytest.mark.anyio
async def test_robots_disallow_rules_enforced():
    robots_txt = """User-agent: *\nDisallow: /private"""
    client = MockClient([MockResponse(200, robots_txt)])
    handler = RobotsHandler(client, "TestBot")

    assert not await handler.is_allowed("https://example.com/private/secret")
    assert await handler.is_allowed("https://example.com/public")


@pytest.mark.anyio
async def test_robots_cache_used_for_multiple_urls():
    client = MockClient([MockResponse(404, "")])
    handler = RobotsHandler(client, "TestBot")

    assert await handler.is_allowed("https://example.com/page1")
    assert await handler.is_allowed("https://example.com/page2")

    # robots.txt fetched only once due to caching
    assert client.calls == 1


@pytest.mark.anyio
async def test_robots_missing_or_server_error_defaults_to_allow():
    client = MockClient([MockResponse(503, ""), MockResponse(404, "")])
    handler = RobotsHandler(client, "TestBot")

    assert await handler.is_allowed("https://example.com/page1")
    assert await handler.is_allowed("https://example.com/page2")
