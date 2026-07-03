import json
from unittest.mock import MagicMock

import pytest

from bq.app import BeanQueue
from bq.config import Config


def _make_environ(path: str) -> dict:
    """Build a minimal WSGI environ dict."""
    return {"PATH_INFO": path, "REQUEST_METHOD": "GET"}


@pytest.fixture
def bq():
    """Create a BeanQueue with real Config (no real DB needed)."""
    instance = BeanQueue(config=Config(
        DATABASE_URL="postgresql://test@localhost/test",
    ))
    return instance

class TestHealthzEndpoint:
    """Tests for the /healthz HTTP handler."""

    def test_healthz_returns_200_when_healthy(self, bq):
        bq._health_state = (True, {"state": "RUNNING"})

        start_response = MagicMock()
        result = bq._serve_http_request("42", _make_environ("/healthz"), start_response)

        start_response.assert_called_once_with(
            "200 OK", [("Content-Type", "application/json")]
        )
        body = json.loads(result[0])
        assert body["status"] == "ok"
        assert body["worker_id"] == "42"
        assert body["state"] == "RUNNING"

    def test_healthz_returns_500_when_unhealthy(self, bq):
        bq._health_state = (False, {"state": "SHUTDOWN"})

        start_response = MagicMock()
        result = bq._serve_http_request("42", _make_environ("/healthz"), start_response)

        start_response.assert_called_once_with(
            "500 Internal Server Error",
            [("Content-Type", "application/json")],
        )
        body = json.loads(result[0])
        assert body["status"] == "error"
        assert body["worker_id"] == "42"
        assert body["state"] == "SHUTDOWN"

    def test_healthz_returns_500_before_worker_initialized(self, bq):
        """Before process_tasks runs, _health_ok is False and _health_info is empty."""
        start_response = MagicMock()
        result = bq._serve_http_request("1", _make_environ("/healthz"), start_response)

        start_response.assert_called_once_with(
            "500 Internal Server Error",
            [("Content-Type", "application/json")],
        )
        body = json.loads(result[0])
        assert body["status"] == "error"
        assert body["worker_id"] == "1"

    def test_healthz_does_not_create_db_session(self, bq):
        """The critical fix: /healthz must never touch the DB."""
        bq._health_state = (True, {"state": "RUNNING"})

        bq.make_session = MagicMock()
        start_response = MagicMock()
        bq._serve_http_request("42", _make_environ("/healthz"), start_response)

        bq.make_session.assert_not_called()

    def test_unknown_path_returns_404(self, bq):
        start_response = MagicMock()
        result = bq._serve_http_request("42", _make_environ("/unknown"), start_response)

        start_response.assert_called_once_with(
            "404 NOT FOUND", [("Content-Type", "application/json")]
        )
        body = json.loads(result[0])
        assert body["status"] == "not found"

    def test_404_does_not_create_db_session(self, bq):
        bq.make_session = MagicMock()
        start_response = MagicMock()
        bq._serve_http_request("42", _make_environ("/anything"), start_response)

        bq.make_session.assert_not_called()


class TestHealthStateInitialization:
    """Tests that _health_ok defaults correctly."""

    def test_defaults_to_unhealthy(self, bq):
        assert bq._health_state == (False, {})
