"""End-to-end soak tests for the asyncio worker.

These keep live worker processes running while tasks arrive over time, contend
across workers, fail, retry, and recover from a killed worker. Duration can be
stretched locally with BQ_SOAK_SECONDS (default 8).
"""

import datetime
import os
import time
import uuid

from sqlalchemy import func
from sqlalchemy.orm import Session

from .fixtures.soak_processors import async_db_echo
from .fixtures.soak_processors import async_ping
from .fixtures.soak_processors import async_slow
from .fixtures.soak_processors import boom
from .fixtures.soak_processors import flaky
from .fixtures.soak_processors import ping
from .fixtures.soak_processors import spawn_child
from .helpers import counts_by_state
from .helpers import start_soak_workers
from .helpers import stop_processes
from .helpers import wait_for_done
from .helpers import wait_for_running_workers
from .helpers import wait_until
from bq import models

SOAK_SECONDS = float(os.environ.get("BQ_SOAK_SECONDS", "8"))


def _complete_event_count(db: Session, task_ids: list) -> int:
    return (
        db.query(models.Event)
        .filter(models.Event.task_id.in_(task_ids))
        .filter(models.Event.type == models.EventType.COMPLETE)
        .count()
    )


def _duplicate_complete_events(db: Session, task_ids: list) -> list:
    return (
        db.query(models.Event.task_id, func.count())
        .filter(models.Event.task_id.in_(task_ids))
        .filter(models.Event.type == models.EventType.COMPLETE)
        .group_by(models.Event.task_id)
        .having(func.count() > 1)
        .all()
    )


def test_notify_wakes_idle_worker(db: Session, db_url: str):
    """An idle worker blocked on LISTEN should pick up a new task without waiting out POLL_TIMEOUT."""
    procs = start_soak_workers(
        db_url,
        count=1,
        MAX_CONCURRENT_TASKS=2,
        BATCH_SIZE=2,
        POLL_TIMEOUT=30,
    )
    try:
        wait_for_running_workers(db, 1)
        task = ping.run(n=1)
        db.add(task)
        db.commit()
        begin = time.monotonic()
        wait_for_done(db, [task.id], timeout=5)
        elapsed = time.monotonic() - begin
        db.expire_all()
        assert db.get(models.Task, task.id).result == 1
        assert elapsed < 10, (
            f"worker took {elapsed:.1f}s; LISTEN likely missed NOTIFY and waited on POLL_TIMEOUT"
        )
    finally:
        stop_processes(procs)


def test_worker_survives_repeated_poll_timeouts(db: Session, db_url: str):
    """After several empty POLL_TIMEOUT cycles the worker should still process new work."""
    procs = start_soak_workers(
        db_url,
        count=1,
        MAX_CONCURRENT_TASKS=2,
        BATCH_SIZE=2,
        POLL_TIMEOUT=1,
    )
    try:
        wait_for_running_workers(db, 1)
        time.sleep(2.5)
        task = ping.run(n=7)
        db.add(task)
        db.commit()
        wait_for_done(db, [task.id], timeout=5)
        db.expire_all()
        assert db.get(models.Task, task.id).result == 7
    finally:
        stop_processes(procs)


def test_skip_locked_no_double_complete(db: Session, db_url: str):
    """Many workers competing for the same queue must process each task exactly once."""
    procs = start_soak_workers(
        db_url,
        count=4,
        MAX_CONCURRENT_TASKS=4,
        BATCH_SIZE=4,
        POLL_TIMEOUT=5,
    )
    try:
        wait_for_running_workers(db, 4)
        task_ids = []
        for _ in range(160):
            task = async_ping.run(n=str(uuid.uuid4()))
            db.add(task)
            db.flush()
            task_ids.append(task.id)
        db.commit()
        wait_for_done(db, task_ids, timeout=20)
        db.expire_all()
        counts = counts_by_state(db, task_ids)
        assert counts.get(models.TaskState.DONE) == len(task_ids)
        assert counts.get(models.TaskState.PROCESSING, 0) == 0
        assert counts.get(models.TaskState.FAILED, 0) == 0
        assert _complete_event_count(db, task_ids) == len(task_ids)
        assert _duplicate_complete_events(db, task_ids) == []
    finally:
        stop_processes(procs)


def test_continuous_enqueue_soak(db: Session, db_url: str):
    """Keep enqueueing while workers are already draining so LISTEN/dispatch stay busy."""
    procs = start_soak_workers(
        db_url,
        count=3,
        MAX_CONCURRENT_TASKS=4,
        BATCH_SIZE=3,
        POLL_TIMEOUT=5,
    )
    try:
        wait_for_running_workers(db, 3)
        task_ids = []
        deadline = time.monotonic() + SOAK_SECONDS
        n = 0
        while time.monotonic() < deadline:
            for _ in range(12):
                task = ping.run(n=n) if n % 3 else async_ping.run(n=n)
                db.add(task)
                db.flush()
                task_ids.append(task.id)
                n += 1
            db.commit()
            time.sleep(0.25)
        wait_for_done(db, task_ids, timeout=20)
        db.expire_all()
        counts = counts_by_state(db, task_ids)
        assert counts.get(models.TaskState.DONE) == len(task_ids)
        assert counts.get(models.TaskState.PROCESSING, 0) == 0
        assert _complete_event_count(db, task_ids) == len(task_ids)
        assert _duplicate_complete_events(db, task_ids) == []
        for task in db.query(models.Task).filter(models.Task.id.in_(task_ids)).all():
            assert task.result == task.kwargs["n"]
    finally:
        stop_processes(procs)


def test_dead_worker_tasks_are_rescheduled(db: Session, db_url: str):
    """Tasks held by a killed worker must be rescheduled by a surviving worker's heartbeat."""
    heartbeat = dict(
        MAX_CONCURRENT_TASKS=2,
        BATCH_SIZE=2,
        POLL_TIMEOUT=2,
        WORKER_HEARTBEAT_PERIOD=1,
        WORKER_HEARTBEAT_TIMEOUT=3,
    )
    victim = start_soak_workers(db_url, count=1, **heartbeat)
    survivor: list = []
    try:
        wait_for_running_workers(db, 1)
        task_ids = []
        for i in range(2):
            task = async_slow.run(n=i, sleep_time=3)
            db.add(task)
            db.flush()
            task_ids.append(task.id)
        db.commit()
        wait_until(
            db,
            lambda: (
                counts_by_state(db, task_ids).get(models.TaskState.PROCESSING, 0) == 2
            ),
            timeout=8,
            message=lambda: (
                f"tasks never started processing: {counts_by_state(db, task_ids)}"
            ),
        )
        db.expire_all()
        victim_worker_id = db.get(models.Task, task_ids[0]).worker_id
        assert victim_worker_id is not None
        assert all(
            db.get(models.Task, task_id).worker_id == victim_worker_id
            for task_id in task_ids
        )

        survivor = start_soak_workers(db_url, count=1, **heartbeat)
        wait_for_running_workers(db, 2)
        stop_processes(victim)
        wait_for_done(db, task_ids, timeout=20)
        db.expire_all()
        assert counts_by_state(db, task_ids).get(models.TaskState.DONE) == len(task_ids)
        assert (
            db.get(models.Worker, victim_worker_id).state
            == models.WorkerState.NO_HEARTBEAT
        )
        for task in db.query(models.Task).filter(models.Task.id.in_(task_ids)):
            assert task.worker_id != victim_worker_id
    finally:
        stop_processes(victim + survivor)


def test_scheduled_tasks_run_after_delay(db: Session, db_url: str):
    procs = start_soak_workers(
        db_url,
        count=1,
        MAX_CONCURRENT_TASKS=2,
        BATCH_SIZE=2,
        POLL_TIMEOUT=1,
    )
    try:
        wait_for_running_workers(db, 1)
        ready = ping.run(n=1)
        delayed = ping.run(n=2)
        delayed.scheduled_at = datetime.datetime.now(
            datetime.timezone.utc
        ) + datetime.timedelta(seconds=2)
        db.add(ready)
        db.add(delayed)
        db.commit()

        wait_for_done(db, [ready.id], timeout=5)
        db.expire_all()
        assert db.get(models.Task, delayed.id).state == models.TaskState.PENDING

        wait_for_done(db, [ready.id, delayed.id], timeout=8)
        db.expire_all()
        assert db.get(models.Task, delayed.id).result == 2
    finally:
        stop_processes(procs)


def test_async_db_processors_and_child_tasks(db: Session, db_url: str):
    procs = start_soak_workers(
        db_url,
        count=2,
        MAX_CONCURRENT_TASKS=4,
        BATCH_SIZE=4,
        POLL_TIMEOUT=5,
    )
    try:
        wait_for_running_workers(db, 2)
        echo_ids = []
        for i in range(40):
            task = async_db_echo.run(n=i)
            db.add(task)
            db.flush()
            echo_ids.append(task.id)
        parent = spawn_child.run(n=99)
        db.add(parent)
        db.commit()

        wait_for_done(db, echo_ids + [parent.id], timeout=15)
        wait_until(
            db,
            lambda: (
                db.query(models.Task)
                .filter(models.Task.parent_id == parent.id)
                .filter(models.Task.state == models.TaskState.DONE)
                .count()
                == 1
            ),
            timeout=10,
            message="child task was not processed",
        )
        db.expire_all()
        for task in db.query(models.Task).filter(models.Task.id.in_(echo_ids)):
            assert task.result["n"] == task.kwargs["n"]
            assert "now" in task.result
        child = db.query(models.Task).filter(models.Task.parent_id == parent.id).one()
        assert child.result == 99
        assert child.channel == parent.channel
    finally:
        stop_processes(procs)


def test_mixed_failures_do_not_stall_worker(db: Session, db_url: str):
    procs = start_soak_workers(
        db_url,
        count=2,
        MAX_CONCURRENT_TASKS=4,
        BATCH_SIZE=4,
        POLL_TIMEOUT=2,
    )
    try:
        wait_for_running_workers(db, 2)
        ids = []
        expected_failed = 0
        expected_done = 0
        for i in range(30):
            if i % 5 == 0:
                task = boom.run(n=i)
                expected_failed += 1
            elif i % 5 == 1:
                task = flaky.run(n=i)
                expected_done += 1
            else:
                task = ping.run(n=i)
                expected_done += 1
            db.add(task)
            db.flush()
            ids.append(task.id)
        db.commit()
        wait_for_done(db, ids, timeout=20, allow_failed=True)
        db.expire_all()
        counts = counts_by_state(db, ids)
        assert counts.get(models.TaskState.FAILED, 0) == expected_failed
        assert counts.get(models.TaskState.DONE, 0) == expected_done
        assert counts.get(models.TaskState.PROCESSING, 0) == 0
        assert counts.get(models.TaskState.PENDING, 0) == 0
    finally:
        stop_processes(procs)


def test_channel_isolation(db: Session, db_url: str):
    procs = start_soak_workers(
        db_url,
        count=1,
        channels=("soak-tests",),
        MAX_CONCURRENT_TASKS=2,
        BATCH_SIZE=2,
        POLL_TIMEOUT=2,
    )
    try:
        wait_for_running_workers(db, 1)
        ours = ping.run(n=1)
        other = ping.run(n=2)
        other.channel = "other-channel"
        db.add(ours)
        db.add(other)
        db.commit()
        wait_for_done(db, [ours.id], timeout=8)
        db.expire_all()
        assert db.get(models.Task, other.id).state == models.TaskState.PENDING
        assert db.get(models.Task, ours.id).state == models.TaskState.DONE
    finally:
        stop_processes(procs)
