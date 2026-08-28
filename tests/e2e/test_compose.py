"""Compose-backed BeanQueue end-to-end tests.

Talks to the stack started by tests/e2e/compose.yaml: Postgres, three worker
containers, real task inserts, stress load, a SIGKILL'd worker, and a graceful
docker stop so leftover work is cleaned up.
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
import uuid

import httpx
import pytest
from sqlalchemy.orm import Session

from .helpers import BURST
from .helpers import ComposeStack
from .helpers import STRESS_SECONDS
from .helpers import WORKER_CONTAINERS
from .helpers import busiest_hostname
from .helpers import clear_queue
from .helpers import complete_event_count
from .helpers import counts_by_state
from .helpers import duplicate_complete_events
from .helpers import health_urls
from .helpers import running_workers
from .helpers import wait_for_finished
from .helpers import wait_until
from .processors import async_db_echo
from .processors import async_ping
from .processors import async_slow
from .processors import boom
from .processors import flaky
from .processors import ping
from .processors import spawn_child
from bq import models

pytestmark = pytest.mark.e2e


def test_health(db: Session, db_url: str):
    wait_until(
        lambda: len(running_workers(db)) >= 3,
        timeout=30,
        message="expected 3 running workers",
    )
    for url in health_urls():

        def _healthz_ok(u: str = url) -> bool:
            try:
                return httpx.get(u, timeout=2).status_code == 200
            except httpx.HTTPError:
                return False

        wait_until(
            _healthz_ok,
            timeout=20,
            message=f"healthz never became ok: {url}",
        )

    subprocess.check_call(
        [
            "bq",
            "--disable-rich-log",
            "submit",
            "e2e",
            "tests.e2e.processors",
            "ping",
            "-k",
            '{"n": -1}',
        ],
        env={**os.environ, "BQ_DATABASE_URL": db_url},
    )
    db.expire_all()
    task = db.query(models.Task).filter(models.Task.kwargs.contains({"n": -1})).one()
    wait_for_finished(db, [task.id], timeout=10)
    db.expire_all()
    assert db.get(models.Task, task.id).result == -1


def test_burst(db: Session):
    clear_queue(db)
    ids: list[uuid.UUID] = []
    for i in range(BURST):
        if i % 11 == 0:
            task = async_db_echo.run(n=i)
        elif i % 17 == 0:
            task = spawn_child.run(n=i)
        elif i % 2 == 0:
            task = ping.run(n=i)
        else:
            task = async_ping.run(n=i)
        db.add(task)
        db.flush()
        ids.append(task.id)
    db.commit()
    wait_for_finished(db, ids, timeout=60)
    expected_children = sum(1 for i in range(BURST) if i % 17 == 0 and i % 11 != 0)

    def _children_done() -> bool:
        db.expire_all()
        return (
            db.query(models.Task)
            .filter(models.Task.parent_id.in_(ids))
            .filter(models.Task.state == models.TaskState.DONE)
            .count()
            == expected_children
        )

    wait_until(
        _children_done,
        timeout=30,
        message="child tasks were not processed",
    )
    db.expire_all()
    counts = counts_by_state(db, ids)
    assert counts.get(models.TaskState.DONE) == len(ids), counts
    assert counts.get(models.TaskState.PROCESSING, 0) == 0
    assert duplicate_complete_events(db, ids) == []
    assert complete_event_count(db, ids) == len(ids)
    children = (
        db.query(models.Task)
        .filter(models.Task.parent_id.in_(ids))
        .filter(models.Task.state == models.TaskState.DONE)
        .count()
    )
    assert children == expected_children, (children, expected_children)


def test_stress(db: Session, compose_stack: ComposeStack):
    clear_queue(db)
    ids: list[uuid.UUID] = []
    lock = threading.Lock()
    stop_at = time.monotonic() + STRESS_SECONDS
    expected_failed = {"n": 0}

    def _produce():
        n = 0
        session = compose_stack.session_factory()
        try:
            while time.monotonic() < stop_at:
                for _ in range(8):
                    if n % 10 == 0:
                        task = boom.run(n=n)
                        with lock:
                            expected_failed["n"] += 1
                    elif n % 10 == 1:
                        task = flaky.run(n=n)
                    elif n % 2 == 0:
                        task = ping.run(n=n)
                    else:
                        task = async_ping.run(n=n)
                    session.add(task)
                    session.flush()
                    with lock:
                        ids.append(task.id)
                    n += 1
                session.commit()
                time.sleep(0.05)
        finally:
            session.close()

    threads = [threading.Thread(target=_produce, daemon=True) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    wait_for_finished(db, ids, timeout=90, allow_failed=True)
    db.expire_all()
    counts = counts_by_state(db, ids)
    assert counts.get(models.TaskState.FAILED, 0) == expected_failed["n"], counts
    assert counts.get(models.TaskState.DONE, 0) == len(ids) - expected_failed["n"]
    assert counts.get(models.TaskState.PROCESSING, 0) == 0
    assert counts.get(models.TaskState.PENDING, 0) == 0
    done_ids = [
        task.id
        for task in db.query(models.Task)
        .filter(models.Task.id.in_(ids))
        .filter(models.Task.state == models.TaskState.DONE)
    ]
    assert duplicate_complete_events(db, done_ids) == []


def test_dead_worker(db: Session, compose_stack: ComposeStack):
    clear_queue(db)
    ids: list[uuid.UUID] = []
    for i in range(6):
        task = async_slow.run(n=i, sleep_time=8)
        db.add(task)
        db.flush()
        ids.append(task.id)
    db.commit()
    wait_until(
        lambda: counts_by_state(db, ids).get(models.TaskState.PROCESSING, 0) >= 3,
        timeout=15,
        message=f"slow tasks never started: {counts_by_state(db, ids)}",
    )
    db.expire_all()
    hostname = busiest_hostname(db, ids)
    container = WORKER_CONTAINERS[hostname]
    worker_row = db.query(models.Worker).filter(models.Worker.name == hostname).one()
    compose_stack.kill(container)

    wait_for_finished(db, ids, timeout=45)
    db.expire_all()
    assert counts_by_state(db, ids).get(models.TaskState.DONE) == len(ids)
    assert duplicate_complete_events(db, ids) == []

    def _no_heartbeat() -> bool:
        db.expire_all()
        worker = db.get(models.Worker, worker_row.id)
        return worker is not None and worker.state == models.WorkerState.NO_HEARTBEAT

    wait_until(
        _no_heartbeat,
        timeout=20,
        message=lambda: (
            "killed worker was not marked NO_HEARTBEAT: "
            f"{db.get(models.Worker, worker_row.id).state}"
        ),
    )
    for task in db.query(models.Task).filter(models.Task.id.in_(ids)):
        assert task.worker_id != worker_row.id


def test_graceful_shutdown(db: Session, compose_stack: ComposeStack):
    clear_queue(db)
    alive = [worker.name for worker in running_workers(db)]
    assert alive, "no running workers left for graceful shutdown"
    ids: list[uuid.UUID] = []
    for i in range(4):
        task = async_slow.run(n=i, sleep_time=4)
        db.add(task)
        db.flush()
        ids.append(task.id)
    db.commit()
    wait_until(
        lambda: counts_by_state(db, ids).get(models.TaskState.PROCESSING, 0) >= 2,
        timeout=15,
        message=f"slow tasks never started: {counts_by_state(db, ids)}",
    )
    db.expire_all()
    hostname = busiest_hostname(db, ids)
    if hostname not in alive:
        hostname = alive[0]
    container = WORKER_CONTAINERS[hostname]
    worker_row = db.query(models.Worker).filter(models.Worker.name == hostname).one()
    compose_stack.stop(container, timeout=20)

    def _is_shutdown() -> bool:
        db.expire_all()
        worker = db.get(models.Worker, worker_row.id)
        return worker is not None and worker.state == models.WorkerState.SHUTDOWN

    wait_until(
        _is_shutdown,
        timeout=25,
        message=lambda: (
            "stopped worker was not marked SHUTDOWN: "
            f"{db.get(models.Worker, worker_row.id).state}"
        ),
    )
    wait_for_finished(db, ids, timeout=30)
    db.expire_all()
    assert counts_by_state(db, ids).get(models.TaskState.DONE) == len(ids)
    assert counts_by_state(db, ids).get(models.TaskState.PROCESSING, 0) == 0


def test_cleanup(db: Session):
    leftover = (
        db.query(models.Task)
        .filter(models.Task.state == models.TaskState.PROCESSING)
        .count()
    )
    assert leftover == 0, f"{leftover} tasks still PROCESSING after e2e"
    running = running_workers(db)
    assert running, "expected at least one worker still running"
