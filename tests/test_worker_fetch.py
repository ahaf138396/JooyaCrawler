import httpx
import pytest

from crawler.monitoring.metrics_server import SKIPPED_LARGE_BODIES, SKIPPED_NON_HTML
from crawler.worker import Worker, FetchResult


@pytest.mark.asyncio
async def test_fetch_skips_non_html_content():
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=200,
            headers={"Content-Type": "application/json"},
            json={"ok": True},
        )

    transport = httpx.MockTransport(handler)
    worker = Worker(queue=None, mongo=None, worker_id=1)

    before = SKIPPED_NON_HTML.labels(worker="1")._value.get()

    async with httpx.AsyncClient(transport=transport) as client:
        worker.client = client
        result = await worker._fetch("http://example.com/api")

    assert isinstance(result, FetchResult)
    assert result.skipped is True
    assert result.skip_reason == "non_html_content"
    assert SKIPPED_NON_HTML.labels(worker="1")._value.get() == before + 1


@pytest.mark.asyncio
async def test_fetch_skips_large_bodies(monkeypatch):
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=200,
            headers={"Content-Type": "text/html"},
            content=b"x" * 50,
        )

    # Force a very small download limit
    monkeypatch.setattr("crawler.worker.MAX_DOWNLOAD_BYTES", 10)

    transport = httpx.MockTransport(handler)
    worker = Worker(queue=None, mongo=None, worker_id=2)

    before = SKIPPED_LARGE_BODIES.labels(worker="2")._value.get()

    async with httpx.AsyncClient(transport=transport) as client:
        worker.client = client
        result = await worker._fetch("http://example.com/large")

    assert isinstance(result, FetchResult)
    assert result.skipped is True
    assert result.skip_reason == "body_too_large"
    assert SKIPPED_LARGE_BODIES.labels(worker="2")._value.get() == before + 1


@pytest.mark.asyncio
async def test_fetch_returns_html_body():
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=200,
            headers={"Content-Type": "text/html; charset=utf-8"},
            text="<html><body>Hello</body></html>",
        )

    transport = httpx.MockTransport(handler)
    worker = Worker(queue=None, mongo=None, worker_id=3)

    async with httpx.AsyncClient(transport=transport) as client:
        worker.client = client
        result = await worker._fetch("http://example.com")

    assert isinstance(result, FetchResult)
    assert result.skipped is False
    assert "Hello" in result.content
