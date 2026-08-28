import asyncio
import time
import typing
import uuid
from multiprocessing import Process

from sqlalchemy.orm import Session

from bq import models
from bq.config import Config

SOAK_PROCESSOR_PACKAGE = "tests.acceptance.fixtures.soak_processors"
SOAK_CHANNEL = "soak-tests"


def run_soak_worker(
    db_url: str,
    channels: tuple[str, ...] = (SOAK_CHANNEL,),
    **config_kw: typing.Any,
):
    from tests.acceptance.fixtures.soak_processors import app

    app.config = Config(
        PROCESSOR_PACKAGES=[SOAK_PROCESSOR_PACKAGE],
        DATABASE_URL=db_url,
        **config_kw,
    )
    asyncio.run(app.process_tasks(channels=channels))


def start_soak_workers(
    db_url: str,
    count: int = 1,
    channels: tuple[str, ...] = (SOAK_CHANNEL,),
    **config_kw: typing.Any,
) -> list[Process]:
    procs = []
    for _ in range(count):
        proc = Process(
            target=run_soak_worker,
            kwargs={"db_url": db_url, "channels": channels, **config_kw},
        )
        proc.start()
        procs.append(proc)
    return procs


def stop_processes(procs: list[Process], timeout: float = 3):
    for proc in procs:
        if proc.is_alive():
            proc.kill()
        proc.join(timeout)


def counts_by_state(
    db: Session, task_ids: typing.Sequence[uuid.UUID] | None = None
) -> dict[models.TaskState, int]:
    query = db.query(models.Task)
    if task_ids is not None:
        query = query.filter(models.Task.id.in_(task_ids))
    counts: dict[models.TaskState, int] = {}
    for task in query.all():
        counts[task.state] = counts.get(task.state, 0) + 1
    return counts


def wait_until(
    db: Session,
    predicate: typing.Callable[[], bool],
    timeout: float,
    message: str | typing.Callable[[], str],
    interval: float = 0.2,
):
    begin = time.monotonic()
    while True:
        db.expire_all()
        if predicate():
            return
        if time.monotonic() - begin > timeout:
            text = message() if callable(message) else message
            raise TimeoutError(text)
        time.sleep(interval)


def wait_for_running_workers(db: Session, count: int, timeout: float = 8):
    """Wait until `count` workers have registered, then briefly yield so they enter LISTEN."""

    def _running() -> int:
        return (
            db.query(models.Worker)
            .filter(models.Worker.state == models.WorkerState.RUNNING)
            .count()
        )

    wait_until(
        db,
        lambda: _running() >= count,
        timeout=timeout,
        message=lambda: f"expected {count} running workers, found {_running()}",
    )
    time.sleep(0.4)


def wait_for_done(
    db: Session,
    task_ids: typing.Sequence[uuid.UUID],
    timeout: float,
    allow_failed: bool = False,
):
    expected = len(task_ids)

    def _done() -> bool:
        counts = counts_by_state(db, task_ids)
        finished = counts.get(models.TaskState.DONE, 0)
        if allow_failed:
            finished += counts.get(models.TaskState.FAILED, 0)
        return finished == expected

    wait_until(
        db,
        _done,
        timeout=timeout,
        message=lambda: (
            f"timeout waiting for {expected} tasks to finish: "
            f"{counts_by_state(db, task_ids)}"
        ),
    )
