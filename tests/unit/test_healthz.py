import asyncio
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from bq import events
from bq.app import BeanQueue
from bq.metrics import MetricsExtrasNotInstalledError
from bq.metrics import MetricsServer
from bq.metrics import require_metrics_extras


@pytest.fixture
def running_worker() -> MagicMock:
    worker = MagicMock()
    return worker


@pytest.fixture
def app_with_worker(
    running_worker: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> BeanQueue:
    app = BeanQueue()
    db = MagicMock()
    worker_service = MagicMock()
    worker_service.get_worker.return_value = running_worker
    monkeypatch.setattr(app, "make_session", lambda: db)
    monkeypatch.setattr(app, "_make_worker_service", lambda _session: worker_service)
    return app


@pytest.fixture
def metrics_server(app_with_worker: BeanQueue) -> MetricsServer:
    return MetricsServer(app_with_worker, "worker-1")


def test_check_healthz_ok(metrics_server: MetricsServer):
    ok, body = asyncio.run(metrics_server.check_healthz())

    assert ok is True
    assert body["status"] == "ok"


def test_check_healthz_event_receiver_raises(metrics_server: MetricsServer):
    @events.healthz_check.connect
    def _check(sender, worker, session):
        raise ValueError("external service down")

    try:
        ok, body = asyncio.run(metrics_server.check_healthz())
    finally:
        events.healthz_check.disconnect(_check)

    assert ok is False
    assert body == {
        "status": "internal error",
        "worker_id": "worker-1",
        "error": "external service down",
    }


def test_check_healthz_event_receiver_success(metrics_server: MetricsServer):
    @events.healthz_check.connect
    def _check(sender, worker, session):
        return None

    try:
        ok, body = asyncio.run(metrics_server.check_healthz())
    finally:
        events.healthz_check.disconnect(_check)

    assert ok is True
    assert body == {"status": "ok", "worker_id": "worker-1"}


def test_check_healthz_async_event_receiver(metrics_server: MetricsServer):
    @events.healthz_check.connect
    async def _check(sender, worker, session):
        await asyncio.sleep(0)

    try:
        ok, body = asyncio.run(metrics_server.check_healthz())
    finally:
        events.healthz_check.disconnect(_check)

    assert ok is True
    assert body == {"status": "ok", "worker_id": "worker-1"}


def test_check_healthz_mixed_sync_and_async_receivers(metrics_server: MetricsServer):
    calls: list[str] = []

    @events.healthz_check.connect
    def _sync_check(sender, worker, session):
        calls.append("sync")

    @events.healthz_check.connect
    async def _async_check(sender, worker, session):
        calls.append("async")

    try:
        ok, body = asyncio.run(metrics_server.check_healthz())
    finally:
        events.healthz_check.disconnect(_sync_check)
        events.healthz_check.disconnect(_async_check)

    assert ok is True
    assert body == {"status": "ok", "worker_id": "worker-1"}
    assert set(calls) == {"sync", "async"}


def test_healthz_endpoint(metrics_server: MetricsServer):
    client = TestClient(metrics_server.create_app())
    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_require_metrics_extras_missing(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("bq.metrics.find_spec", lambda name: None)

    with pytest.raises(MetricsExtrasNotInstalledError, match="beanqueue\\[metrics\\]"):
        require_metrics_extras()
