from aiohttp import web
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    generate_latest,
    Counter,
    Gauge,
    Histogram,
)

# -------------------------
# Worker-Level Metrics
# -------------------------

# تعداد صفحات پردازش شده موفق
WORKER_PROCESSED = Counter(
    "jooya_worker_processed_total",
    "Successfully processed pages",
    ["worker_id"],
)

# تعداد خطاها
WORKER_FAILED = Counter(
    "jooya_worker_failed_total",
    "Failed page processing",
    ["worker_id"],
)

# Worker فعال (1 یعنی فعال)
WORKER_ACTIVE = Gauge(
    "jooya_worker_active",
    "Worker active state",
    ["worker_id"],
)

# -------------------------
# Request Metrics (Optional)
# -------------------------

REQUEST_COUNT = Counter(
    "jooya_requests_total",
    "Total HTTP requests",
    ["worker"]
)

FAILED_REQUESTS = Counter(
    "jooya_failed_requests_total",
    "Failed HTTP requests",
    ["worker"]
)

CRAWLED_PAGES = Counter(
    "jooya_crawled_pages_total",
    "Successfully crawled pages",
    ["worker"]
)

REQUEST_LATENCY = Histogram(
    "jooya_request_latency_seconds",
    "Time to fetch a page",
    ["worker"]
)

# -------------------------
# Queue Metrics
# -------------------------

QUEUE_PENDING = Gauge(
    "jooya_queue_pending",
    "Number of URLs waiting in queue"
)


# -------------------------
# /metrics endpoint
# -------------------------

async def metrics_handler(request):
    data = generate_latest()

    # محتوا فقط باید بدون charset باشد
    ctype = CONTENT_TYPE_LATEST.split(";")[0]

    return web.Response(
        body=data,
        content_type=ctype
    )


async def start_metrics_server(port=8000):
    app = web.Application()
    app.router.add_get("/metrics", metrics_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    return runner, site
