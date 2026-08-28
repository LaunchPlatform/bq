"""Compose-backed BeanQueue end-to-end harness.

Talks to the stack started by tests/e2e/compose.yaml: Postgres, three worker
containers, real task inserts, stress load, a SIGKILL'd worker, and a graceful
docker stop so leftover work is cleaned up.
"""

from __future__ import annotations

import logging
import os
import subprocess
import threading
import time
import traceback
import typing
import uuid

import httpx
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from .processors import async_db_echo
from .processors import async_ping
from .processors import async_slow
from .processors import boom
from .processors import flaky
from .processors import ping
from .processors import spawn_child
from bq import models

logger = logging.getLogger("e2e")

DB_URL = os.environ.get("E2E_DATABASE_URL", "postgresql+psycopg://bq:@postgres:5432/bq")
BURST = int(os.environ.get("E2E_BURST", "400"))
STRESS_SECONDS = float(os.environ.get("E2E_STRESS_SECONDS", "12"))


def _health_urls() -> list[str]:
    raw = os.environ.get(
        "E2E_WORKER_HEALTH_URLS",
        "http://worker-a:8000/healthz,http://worker-b:8000/healthz,http://worker-c:8000/healthz",
    )
    return [item.strip() for item in raw.split(",") if item.strip()]


def _container_map() -> dict[str, str]:
    raw = os.environ.get(
        "E2E_WORKER_CONTAINERS",
        "worker-a:bqe2e-worker-a,worker-b:bqe2e-worker-b,worker-c:bqe2e-worker-c",
    )
    mapping: dict[str, str] = {}
    for part in raw.split(","):
        host, _, name = part.partition(":")
        mapping[host.strip()] = name.strip()
    return mapping


def _docker():
    import docker

    return docker.from_env()


def _session_factory() -> sessionmaker:
    engine = create_engine(DB_URL, pool_pre_ping=True)
    return sessionmaker(bind=engine, expire_on_commit=False)


def wait_until(
    predicate: typing.Callable[[], bool],
    timeout: float,
    message: str | typing.Callable[[], str],
    interval: float = 0.25,
):
    begin = time.monotonic()
    while True:
        if predicate():
            return
        if time.monotonic() - begin > timeout:
            text_msg = message() if callable(message) else message
            raise TimeoutError(text_msg)
        time.sleep(interval)


def counts_by_state(
    db: Session, task_ids: typing.Sequence[uuid.UUID]
) -> dict[models.TaskState, int]:
    rows = (
        db.query(models.Task.state, func.count())
        .filter(models.Task.id.in_(task_ids))
        .group_by(models.Task.state)
        .all()
    )
    return {state: n for state, n in rows}


def complete_event_count(db: Session, task_ids: typing.Sequence[uuid.UUID]) -> int:
    return (
        db.query(models.Event)
        .filter(models.Event.task_id.in_(task_ids))
        .filter(models.Event.type == models.EventType.COMPLETE)
        .count()
    )


def duplicate_complete_events(
    db: Session, task_ids: typing.Sequence[uuid.UUID]
) -> list:
    return (
        db.query(models.Event.task_id, func.count())
        .filter(models.Event.task_id.in_(task_ids))
        .filter(models.Event.type == models.EventType.COMPLETE)
        .group_by(models.Event.task_id)
        .having(func.count() > 1)
        .all()
    )


def wait_for_finished(
    db: Session,
    task_ids: typing.Sequence[uuid.UUID],
    timeout: float,
    allow_failed: bool = False,
):
    expected = len(task_ids)

    def _done() -> bool:
        db.expire_all()
        counts = counts_by_state(db, task_ids)
        finished = counts.get(models.TaskState.DONE, 0)
        if allow_failed:
            finished += counts.get(models.TaskState.FAILED, 0)
        return finished == expected

    wait_until(
        _done,
        timeout=timeout,
        message=lambda: (
            f"timeout waiting for {expected} tasks: {counts_by_state(db, task_ids)}"
        ),
    )


def clear_queue(db: Session):
    db.query(models.Event).delete()
    db.query(models.Task).delete()
    db.commit()


def running_workers(db: Session) -> list[models.Worker]:
    db.expire_all()
    return (
        db.query(models.Worker)
        .filter(models.Worker.state == models.WorkerState.RUNNING)
        .all()
    )


def wait_for_postgres():
    factory = _session_factory()
    begin = time.monotonic()
    last_error: Exception | None = None
    while True:
        db = factory()
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


def scenario_health(db: Session):
    wait_until(
        lambda: len(running_workers(db)) >= 3,
        timeout=30,
        message="expected 3 running workers",
    )
    for url in _health_urls():
        wait_until(
            lambda u=url: httpx.get(u, timeout=2).status_code == 200,
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
        env={**os.environ, "BQ_DATABASE_URL": DB_URL},
    )
    db.expire_all()
    task = db.query(models.Task).filter(models.Task.kwargs.contains({"n": -1})).one()
    wait_for_finished(db, [task.id], timeout=10)
    db.expire_all()
    assert db.get(models.Task, task.id).result == -1


def scenario_burst(db: Session):
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
    expected_children = sum(
        1 for i in range(BURST) if i % 17 == 0 and i % 11 != 0
    )

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
    logger.info("burst finished %s tasks + %s children", len(ids), children)


def scenario_stress(db: Session):
    clear_queue(db)
    ids: list[uuid.UUID] = []
    lock = threading.Lock()
    stop_at = time.monotonic() + STRESS_SECONDS
    expected_failed = {"n": 0}

    def _produce():
        factory = _session_factory()
        n = 0
        session = factory()
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
    logger.info("stress enqueued %s tasks in %ss", len(ids), STRESS_SECONDS)
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
    logger.info("stress drained %s", counts)


def _busiest_hostname(db: Session, task_ids: list[uuid.UUID]) -> str:
    rows = (
        db.query(models.Worker.name, func.count())
        .join(models.Task, models.Task.worker_id == models.Worker.id)
        .filter(models.Task.id.in_(task_ids))
        .filter(models.Task.state == models.TaskState.PROCESSING)
        .group_by(models.Worker.name)
        .all()
    )
    assert rows, "no processing tasks to attribute to a worker"
    return max(rows, key=lambda row: row[1])[0]


def scenario_dead_worker(db: Session):
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
    hostname = _busiest_hostname(db, ids)
    container = _container_map()[hostname]
    worker_row = db.query(models.Worker).filter(models.Worker.name == hostname).one()
    logger.info("SIGKILL worker %s (%s)", hostname, container)
    _docker().containers.get(container).kill()

    wait_for_finished(db, ids, timeout=45)
    db.expire_all()
    assert counts_by_state(db, ids).get(models.TaskState.DONE) == len(ids)
    assert duplicate_complete_events(db, ids) == []
    wait_until(
        lambda: (
            db.get(models.Worker, worker_row.id).state
            == models.WorkerState.NO_HEARTBEAT
        ),
        timeout=20,
        message=(
            "killed worker was not marked NO_HEARTBEAT: "
            f"{db.get(models.Worker, worker_row.id).state}"
        ),
    )
    for task in db.query(models.Task).filter(models.Task.id.in_(ids)):
        assert task.worker_id != worker_row.id
    logger.info("dead worker %s rescheduled and completed", hostname)


def scenario_graceful_shutdown(db: Session):
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
    hostname = _busiest_hostname(db, ids)
    if hostname not in alive:
        hostname = alive[0]
    container = _container_map()[hostname]
    worker_row = db.query(models.Worker).filter(models.Worker.name == hostname).one()
    logger.info("SIGTERM/stop worker %s (%s)", hostname, container)
    _docker().containers.get(container).stop(timeout=20)

    wait_until(
        lambda: (
            db.get(models.Worker, worker_row.id).state == models.WorkerState.SHUTDOWN
        ),
        timeout=25,
        message=(
            "stopped worker was not marked SHUTDOWN: "
            f"{db.get(models.Worker, worker_row.id).state}"
        ),
    )
    wait_for_finished(db, ids, timeout=30)
    db.expire_all()
    assert counts_by_state(db, ids).get(models.TaskState.DONE) == len(ids)
    assert counts_by_state(db, ids).get(models.TaskState.PROCESSING, 0) == 0
    logger.info("graceful shutdown cleaned up in-flight tasks")


def scenario_cleanup(db: Session):
    leftover = (
        db.query(models.Task)
        .filter(models.Task.state == models.TaskState.PROCESSING)
        .count()
    )
    assert leftover == 0, f"{leftover} tasks still PROCESSING after e2e"
    running = running_workers(db)
    assert running, "expected at least one worker still running"
    logger.info("cleanup ok: %s running worker(s), 0 processing tasks", len(running))


SCENARIOS: list[tuple[str, typing.Callable[[Session], None]]] = [
    ("health", scenario_health),
    ("burst", scenario_burst),
    ("stress", scenario_stress),
    ("dead_worker", scenario_dead_worker),
    ("graceful_shutdown", scenario_graceful_shutdown),
    ("cleanup", scenario_cleanup),
]


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    factory = _session_factory()
    wait_for_postgres()
    failed: list[str] = []
    for name, fn in SCENARIOS:
        logger.info("=== %s ===", name)
        db = factory()
        try:
            fn(db)
            logger.info("PASS %s", name)
        except Exception:
            logger.exception("FAIL %s", name)
            traceback.print_exc()
            failed.append(name)
        finally:
            db.close()
    if failed:
        raise SystemExit(f"e2e failed: {', '.join(failed)}")
    logger.info("all e2e scenarios passed")


if __name__ == "__main__":
    main()
