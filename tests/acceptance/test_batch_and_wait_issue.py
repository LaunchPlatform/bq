"""
Test that demonstrates the batch-and-wait issue that was fixed in commit 701d8de.

The issue occurs when:
1. BATCH_SIZE is small (e.g., 1-3)
2. Multiple threads available
3. Many tasks in queue

Old behavior (batch-and-wait):
- Fetch BATCH_SIZE tasks (e.g., 3)
- Submit all 3 to executor
- WAIT for all 3 to complete
- Then fetch next batch

This causes threads to sit idle if BATCH_SIZE < MAX_WORKER_THREADS!

New behavior (continuous feeding):
- Continuously check for completed futures
- Dispatch new tasks as soon as capacity is available
- Keep all threads busy
"""
import asyncio
import datetime
import time
from multiprocessing import Process

from sqlalchemy.orm import Session

from .fixtures.thread_processors import app, slow_task
from bq import models
from bq.config import Config


def run_worker_small_batch(db_url: str, max_workers: int, batch_size: int):
    """Run worker with small batch size to expose batch-and-wait issue."""
    app.config = Config(
        PROCESSOR_PACKAGES=["tests.acceptance.fixtures.thread_processors"],
        DATABASE_URL=db_url,
        MAX_CONCURRENT_TASKS=max_workers,
        BATCH_SIZE=batch_size,  # Small batch size
        POLL_TIMEOUT=20,
    )
    asyncio.run(app.process_tasks(channels=("thread-tests",)))


def test_small_batch_size_with_many_threads(db: Session, db_url: str):
    """
    Test the batch-and-wait issue with small BATCH_SIZE.

    Setup:
    - 4 worker threads
    - BATCH_SIZE = 1 (only fetch 1 task at a time)
    - 8 tasks total, each taking 0.5 seconds

    Old behavior (batch-and-wait):
    - Iteration 1: Fetch 1 task, submit to executor, wait for it to complete (0.5s)
    - Iteration 2: Fetch 1 task, submit to executor, wait for it to complete (0.5s)
    - ... repeat 8 times
    - Total: 8 * 0.5s = 4+ seconds (SERIAL despite having 4 threads!)

    New behavior (continuous feeding):
    - Fetch task as soon as any thread is free
    - All 4 threads stay busy
    - 8 tasks / 4 threads = 2 batches * 0.5s = 1+ seconds (PARALLEL)

    The new implementation should be ~4x faster!
    """
    proc = Process(
        target=run_worker_small_batch,
        args=(db_url, 4, 1),  # 4 threads, BATCH_SIZE=1
    )
    proc.start()

    # Create 8 tasks
    task_count = 8
    for i in range(task_count):
        task = slow_task.run(task_num=i, sleep_time=0.5)
        db.add(task)
    db.commit()

    begin = datetime.datetime.now()
    while True:
        db.expire_all()
        done_tasks = (
            db.query(models.Task)
            .filter(models.Task.state == models.TaskState.DONE)
            .count()
        )
        if done_tasks == task_count:
            break
        delta = datetime.datetime.now() - begin
        if delta.total_seconds() > 20:
            raise TimeoutError(f"Timeout. Only {done_tasks}/{task_count} completed")
        time.sleep(0.5)

    elapsed = (datetime.datetime.now() - begin).total_seconds()

    assert done_tasks == task_count

    # With continuous feeding (new behavior): 8 tasks / 4 threads = 2 batches * 0.5s = ~1s
    # With batch-and-wait (old behavior): BATCH_SIZE=1 means serial = 8 * 0.5s = ~4s
    #
    # The fix should make this much faster!
    print(f"\n8 tasks with 4 threads but BATCH_SIZE=1: {elapsed:.2f}s")

    # New implementation should complete in ~1-2 seconds
    # Old implementation would take ~4+ seconds (essentially serial)
    if elapsed < 2.5:
        print(f"✓ PASS - Continuous feeding working ({elapsed:.2f}s < 2.5s)")
    else:
        print(f"✗ FAIL - Batch-and-wait issue detected ({elapsed:.2f}s >= 2.5s)")
        raise AssertionError(
            f"Tasks took {elapsed:.2f}s with BATCH_SIZE=1 and 4 threads. "
            f"This suggests batch-and-wait behavior (serial execution despite threads). "
            f"Expected <2.5s with continuous feeding."
        )

    proc.kill()
    proc.join(3)


def test_tape_config_scenario(db: Session, db_url: str):
    """
    Test with tape's actual configuration: MAX_WORKER_THREADS=3, BATCH_SIZE=3.

    Setup:
    - 3 worker threads
    - BATCH_SIZE = 3
    - 12 tasks total, each taking 0.5 seconds

    Old behavior:
    - Fetch 3, submit 3, wait for all 3 (0.5s)
    - Fetch 3, submit 3, wait for all 3 (0.5s)
    - Fetch 3, submit 3, wait for all 3 (0.5s)
    - Fetch 3, submit 3, wait for all 3 (0.5s)
    - Total: 4 * 0.5s = 2+ seconds

    New behavior:
    - Should be similar since BATCH_SIZE matches thread count
    - But more efficient with task arrival/completion timing

    This test is less dramatic than test_small_batch_size_with_many_threads
    but still demonstrates the improvement.
    """
    proc = Process(
        target=run_worker_small_batch,
        args=(db_url, 3, 3),  # Tape's config: 3 threads, BATCH_SIZE=3
    )
    proc.start()

    # Create 12 tasks
    task_count = 12
    for i in range(task_count):
        task = slow_task.run(task_num=i, sleep_time=0.5)
        db.add(task)
    db.commit()

    begin = datetime.datetime.now()
    while True:
        db.expire_all()
        done_tasks = (
            db.query(models.Task)
            .filter(models.Task.state == models.TaskState.DONE)
            .count()
        )
        if done_tasks == task_count:
            break
        delta = datetime.datetime.now() - begin
        if delta.total_seconds() > 20:
            raise TimeoutError(f"Timeout. Only {done_tasks}/{task_count} completed")
        time.sleep(0.5)

    elapsed = (datetime.datetime.now() - begin).total_seconds()

    assert done_tasks == task_count

    # Expected: 12 tasks / 3 threads = 4 batches * 0.5s = ~2s
    # Should complete in roughly 2-3 seconds
    print(f"\n12 tasks with 3 threads and BATCH_SIZE=3 (tape config): {elapsed:.2f}s")

    assert elapsed < 4, f"Expected ~2s, but took {elapsed:.2f}s"
    print(f"✓ PASS - Tape config scenario: {elapsed:.2f}s")

    proc.kill()
    proc.join(3)


def test_worst_case_batch_one(db: Session, db_url: str):
    """
    Worst case: BATCH_SIZE=1 with many tasks and many threads.

    This would be EXTREMELY slow with batch-and-wait (essentially serial),
    but should be fast with continuous feeding.
    """
    proc = Process(
        target=run_worker_small_batch,
        args=(db_url, 8, 1),  # 8 threads but BATCH_SIZE=1
    )
    proc.start()

    # Create 16 short tasks
    task_count = 16
    for i in range(task_count):
        task = slow_task.run(task_num=i, sleep_time=0.3)
        db.add(task)
    db.commit()

    begin = datetime.datetime.now()
    while True:
        db.expire_all()
        done_tasks = (
            db.query(models.Task)
            .filter(models.Task.state == models.TaskState.DONE)
            .count()
        )
        if done_tasks == task_count:
            break
        delta = datetime.datetime.now() - begin
        if delta.total_seconds() > 20:
            raise TimeoutError(f"Timeout. Only {done_tasks}/{task_count} completed")
        time.sleep(0.5)

    elapsed = (datetime.datetime.now() - begin).total_seconds()

    assert done_tasks == task_count

    # With continuous feeding: 16 tasks / 8 threads = 2 batches * 0.3s = ~0.6-1s
    # With batch-and-wait: BATCH_SIZE=1 = serial = 16 * 0.3s = ~4.8s
    print(f"\n16 tasks with 8 threads but BATCH_SIZE=1: {elapsed:.2f}s")

    if elapsed < 2:
        print(f"✓ PASS - Continuous feeding working excellently ({elapsed:.2f}s)")
    else:
        print(f"✗ FAIL - Batch-and-wait detected ({elapsed:.2f}s)")
        raise AssertionError(
            f"With 8 threads and BATCH_SIZE=1, expected <2s, got {elapsed:.2f}s. "
            f"This indicates batch-and-wait serial behavior."
        )

    proc.kill()
    proc.join(3)
