from aiohttp import web
from prometheus_client import (
    Gauge,
    Counter,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

# ---------- Metrics ----------

CRAWLED_PAGES = Counter(
    "jooya_crawler_pages_total",
    "Total crawled pages successfully",
)

CRAWL_ERRORS = Counter(
    "jooya_crawler_errors_total",
    "Total crawl errors",
)

CRAWL_LATENCY = Histogram(
    "jooya_crawler_latency_seconds",
    "Time spent processing a page",
    buckets=[0.1, 0.3, 0.5, 1, 2, 5, 10]
)

EXTRACTED_LINKS = Histogram(
    "jooya_extracted_links_per_page",
    "Number of extracted outbound links per page",
    buckets=[0, 5, 10, 20, 50, 100, 500, 1000]
)

QUEUE_PENDING = Gauge(
    "jooya_queue_pending",
    "Number of pending URLs in queue",
)

WORKER_ACTIVE = Gauge(
    "jooya_worker_active",
    "Number of active workers",
)

def metrics_handler(request):
    data = generate_latest()
    return web.Response(body=data, content_type=CONTENT_TYPE_LATEST)

async def start_metrics_server(port: int = 8000):
    app = web.Application()
    app.router.add_get("/metrics", metrics_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"[Metrics] Prometheus metrics server running on port {port}")
