import pytest

from crawler.storage.radar_queue_manager import (
    FrontierTask,
    RadarQueueManager,
    STATUS_DONE,
    STATUS_SCHEDULED,
)


class RecordingConnection:
    def __init__(self, *, fetchrow_result=None, fetchval_result=0):
        self.fetchrow_result = fetchrow_result
        self.fetchval_result = fetchval_result
        self.executed = []
        self.fetchval_calls = []
        self.fetchrow_calls = []

    async def execute(self, query, *args):
        self.executed.append((query.strip(), args))

    async def fetchval(self, *args):
        self.fetchval_calls.append(args)
        return self.fetchval_result

    async def fetchrow(self, *args):
        self.fetchrow_calls.append(args)
        return self.fetchrow_result


class AcquireContext:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


class DummyPool:
    def __init__(self, connection):
        self.connection = connection
        self.closed = False

    def acquire(self):
        return AcquireContext(self.connection)

    async def close(self):
        self.closed = True


@pytest.mark.anyio
async def test_connect_normalizes_radar_url(monkeypatch):
    created = {}

    async def fake_create_pool(dsn, min_size, max_size):
        created["dsn"] = dsn
        created["min_size"] = min_size
        created["max_size"] = max_size
        return DummyPool(RecordingConnection())

    monkeypatch.setenv("RADAR_DATABASE_URL", "postgresql+psycopg2://user:pw@host:5432/db")
    monkeypatch.setattr(
        "crawler.storage.radar_queue_manager.asyncpg.create_pool", fake_create_pool
    )

    manager = RadarQueueManager()
    await manager.connect()

    assert created["dsn"] == "postgresql://user:pw@host:5432/db"
    assert created["min_size"] == 1
    assert created["max_size"] == 10
    assert isinstance(manager.pool, DummyPool)


@pytest.mark.anyio
async def test_enqueue_url_respects_max_depth(monkeypatch):
    manager = RadarQueueManager(max_depth=1)
    conn = RecordingConnection()
    manager.pool = DummyPool(conn)

    await manager.enqueue_url("https://example.com/depth2", source_id=1, depth=2, priority=0)

    assert conn.executed == []


@pytest.mark.anyio
async def test_enqueue_url_persists_when_depth_allowed():
    manager = RadarQueueManager(max_depth=None)
    conn = RecordingConnection()
    manager.pool = DummyPool(conn)

    await manager.enqueue_url(
        "https://example.com/page",
        source_id=42,
        depth=0,
        priority=5,
    )

    assert len(conn.executed) == 1
    _, args = conn.executed[0]
    assert args == (
        "https://example.com/page",
        42,
        0,
        5,
        STATUS_SCHEDULED,
    )


@pytest.mark.anyio
async def test_dequeue_task_returns_frontier_task():
    manager = RadarQueueManager(max_depth=None)
    row = {
        "id": 10,
        "url": "https://example.com",
        "source_id": 1,
        "depth": 0,
        "priority": 3,
    }
    conn = RecordingConnection(fetchrow_result=row)
    manager.pool = DummyPool(conn)

    task = await manager.dequeue_task()

    assert isinstance(task, FrontierTask)
    assert task.id == 10
    assert task.url == row["url"]
    assert task.priority == row["priority"]
    assert conn.fetchrow_calls  # ensures query executed


@pytest.mark.anyio
async def test_count_scheduled_without_pool_is_zero():
    manager = RadarQueueManager(max_depth=None)
    manager.pool = None

    count = await manager.count_scheduled()
    assert count == 0


@pytest.mark.anyio
async def test_mark_done_and_failed_execute_statements():
    manager = RadarQueueManager(max_depth=None)
    conn = RecordingConnection()
    manager.pool = DummyPool(conn)

    await manager.mark_done(5, 200)
    await manager.mark_failed(7, status_code=500, error_code="TIMEOUT", error_category="network")

    assert len(conn.executed) == 2
    first_query, first_args = conn.executed[0]
    assert "UPDATE urls_frontier" in first_query
    assert first_args[0] == STATUS_DONE

    second_query, second_args = conn.executed[1]
    assert "fail_count" in second_query
    assert second_args[0] == STATUS_SCHEDULED
    assert second_args[1] == 500
    assert second_args[2] == "TIMEOUT"
    assert second_args[3] == "network"
    assert second_args[4] == 7
