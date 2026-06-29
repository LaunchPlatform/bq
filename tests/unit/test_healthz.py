import json
from unittest.mock import MagicMock

import pytest

from bq import events
from bq import models
from bq.app import BeanQueue


@pytest.fixture
def running_worker() -> MagicMock:
    worker = MagicMock()
    worker.state = models.WorkerState.RUNNING
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


def test_check_healthz_ok(app_with_worker: BeanQueue, running_worker: MagicMock):
    ok, body = app_with_worker._check_healthz("worker-1")

    assert ok is True
    assert body == {"status": "ok", "worker_id": "worker-1"}


def test_check_healthz_bad_worker_state(
    app_with_worker: BeanQueue, running_worker: MagicMock
):
    running_worker.state = models.WorkerState.SHUTDOWN

    ok, body = app_with_worker._check_healthz("worker-1")

    assert ok is False
    assert body == {
        "status": "internal error",
        "worker_id": "worker-1",
        "state": str(models.WorkerState.SHUTDOWN),
    }


def test_check_healthz_init_callback_raises(app_with_worker: BeanQueue):
    def failing_check(_app, _worker, _session):
        raise RuntimeError("queue not ready")

    app_with_worker._healthz_check = failing_check

    ok, body = app_with_worker._check_healthz("worker-1")

    assert ok is False
    assert body == {
        "status": "internal error",
        "worker_id": "worker-1",
        "error": "queue not ready",
    }


def test_check_healthz_event_receiver_raises(app_with_worker: BeanQueue):
    @events.healthz_check.connect
    def _check(sender, worker, session):
        raise ValueError("external service down")

    try:
        ok, body = app_with_worker._check_healthz("worker-1")
    finally:
        events.healthz_check.disconnect(_check)

    assert ok is False
    assert body == {
        "status": "internal error",
        "worker_id": "worker-1",
        "error": "external service down",
    }


def test_check_healthz_init_callback_success(app_with_worker: BeanQueue):
    app_with_worker._healthz_check = lambda _app, _worker, _session: None

    ok, body = app_with_worker._check_healthz("worker-1")

    assert ok is True
    assert body == {"status": "ok", "worker_id": "worker-1"}


def test_serve_http_request_healthz_response(app_with_worker: BeanQueue):
    captured: list[tuple[str, list[tuple[str, str]]]] = []

    body = app_with_worker._serve_http_request(
        "worker-1",
        {"PATH_INFO": "/healthz"},
        lambda status, headers: captured.append((status, headers)),
    )

    assert captured[0][0] == "200 OK"
    assert json.loads(body[0].decode()) == {"status": "ok", "worker_id": "worker-1"}
