"""Docker Compose fixtures for BeanQueue end-to-end tests.

These tests talk to real worker containers, so they are not collected with the
unit/acceptance suite. Run them with:

    uv run python -m pytest tests/e2e -svvvv
"""

from __future__ import annotations

import subprocess
import time
import typing
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from .helpers import WORKER_CONTAINERS
from .helpers import ComposeStack
from .helpers import database_url
from .helpers import make_session_factory

E2E_DIR = Path(__file__).resolve().parent
COMPOSE_FILE = E2E_DIR / "compose.yaml"
PROJECT = "bqe2e"
STACK_SERVICES = ["postgres", "migrate", "worker-a", "worker-b", "worker-c"]

# Keep kill/stop tests after the ones that need all three workers.
_TEST_ORDER = [
    "test_health",
    "test_burst",
    "test_stress",
    "test_dead_worker",
    "test_graceful_shutdown",
    "test_cleanup",
]


def pytest_collection_modifyitems(items: list[pytest.Item]):
    order = {name: index for index, name in enumerate(_TEST_ORDER)}
    items.sort(key=lambda item: order.get(item.name, len(_TEST_ORDER)))


def _docker_argv() -> list[str]:
    for prefix in (["docker"], ["sudo", "docker"]):
        try:
            result = subprocess.run(
                [*prefix, "info"],
                check=False,
                capture_output=True,
                timeout=20,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired):
            continue
        if result.returncode == 0:
            return prefix
    pytest.exit(
        "Docker daemon is not running (need docker or sudo docker).",
        returncode=1,
    )


def _compose_argv(docker: list[str]) -> list[str]:
    return [
        *docker,
        "compose",
        "-f",
        str(COMPOSE_FILE),
        "--project-name",
        PROJECT,
    ]


def _container_health(docker: list[str], name: str) -> str:
    result = subprocess.run(
        [
            *docker,
            "inspect",
            "-f",
            "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
            name,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return "missing"
    return result.stdout.strip() or "missing"


def _wait_for_workers(docker: list[str]):
    print("Waiting for workers to become healthy...", flush=True)
    statuses = ""
    for _ in range(60):
        ready = True
        statuses = ""
        for name in WORKER_CONTAINERS.values():
            status = _container_health(docker, name)
            statuses += f" {name}={status}"
            if status != "healthy":
                ready = False
        if ready:
            print(f"Workers healthy:{statuses}", flush=True)
            return
        time.sleep(2)
    raise TimeoutError(f"Workers did not become healthy:{statuses}")


def _wait_for_postgres(session_factory: sessionmaker):
    begin = time.monotonic()
    last_error: Exception | None = None
    while True:
        db = session_factory()
        try:
            db.execute(text("SELECT 1"))
            return
        except Exception as exc:
            last_error = exc
            if time.monotonic() - begin > 40:
                raise TimeoutError(
                    f"postgres never became ready: {last_error}"
                ) from exc
            time.sleep(0.5)
        finally:
            db.close()


def _compose_down(compose: list[str]):
    subprocess.run(
        [*compose, "down", "-v", "--remove-orphans"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _dump_compose_logs(compose: list[str]):
    print("\n===== compose logs (failure, last 120 lines per service) =====", flush=True)
    subprocess.run(
        [*compose, "logs", "--no-color", "--timestamps", "--tail", "120"],
        check=False,
    )


@pytest.fixture(scope="session")
def compose_stack(request: pytest.FixtureRequest) -> typing.Iterator[ComposeStack]:
    docker = _docker_argv()
    compose = _compose_argv(docker)
    db_url = database_url()
    _compose_down(compose)
    setup_failed = False
    try:
        subprocess.check_call([*compose, "build"])
        subprocess.check_call([*compose, "up", "-d", *STACK_SERVICES])
        _wait_for_workers(docker)
        session_factory = make_session_factory(db_url)
        _wait_for_postgres(session_factory)
        stack = ComposeStack(
            docker=docker,
            compose=compose,
            db_url=db_url,
            session_factory=session_factory,
        )
        yield stack
    except Exception:
        setup_failed = True
        _dump_compose_logs(compose)
        raise
    finally:
        if not setup_failed and request.session.testsfailed:
            _dump_compose_logs(compose)
        _compose_down(compose)


@pytest.fixture
def db(compose_stack: ComposeStack) -> typing.Iterator[Session]:
    session = compose_stack.session_factory()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def db_url(compose_stack: ComposeStack) -> str:
    return compose_stack.db_url
