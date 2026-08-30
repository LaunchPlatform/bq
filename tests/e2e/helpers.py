from __future__ import annotations

import os
import subprocess
import time
import typing
import uuid
from dataclasses import dataclass

from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from bq import models

BURST = int(os.environ.get("E2E_BURST", "400"))
STRESS_SECONDS = float(os.environ.get("E2E_STRESS_SECONDS", "12"))

WORKER_CONTAINERS = {
    "worker-a": "bqe2e-worker-a",
    "worker-b": "bqe2e-worker-b",
    "worker-c": "bqe2e-worker-c",
}


@dataclass
class ComposeStack:
    docker: list[str]
    compose: list[str]
    db_url: str
    session_factory: sessionmaker

    def kill(self, container: str):
        subprocess.check_call(
            [*self.docker, "kill", container],
            stdout=subprocess.DEVNULL,
        )

    def stop(self, container: str, timeout: int = 20):
        subprocess.check_call(
            [*self.docker, "stop", "-t", str(timeout), container],
            stdout=subprocess.DEVNULL,
        )


def database_url() -> str:
    return os.environ.get(
        "E2E_DATABASE_URL", "postgresql+psycopg://bq@127.0.0.1:55432/bq"
    )


def health_urls() -> list[str]:
    raw = os.environ.get(
        "E2E_WORKER_HEALTH_URLS",
        "http://127.0.0.1:18001/healthz,http://127.0.0.1:18002/healthz,http://127.0.0.1:18003/healthz",
    )
    return [item.strip() for item in raw.split(",") if item.strip()]


def make_session_factory(db_url: str) -> sessionmaker:
    engine = create_engine(db_url, pool_pre_ping=True)
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


def busiest_hostname(db: Session, task_ids: list[uuid.UUID]) -> str:
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
