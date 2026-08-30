"""
Realistic integration tests that mirror actual production usage patterns.

These tests are designed to catch issues that might pass in isolated unit tests
but fail in real integrations (like the tape app).
"""

import asyncio
import datetime
import time
from multiprocessing import Process

from sqlalchemy.orm import Session

from .fixtures.thread_processors import app
from .fixtures.thread_processors import record_thread_name
from .fixtures.thread_processors import slow_task
from bq import models
from bq.config import Config


def run_single_worker_with_config(
    db_url: str, max_workers: int, batch_size: int, duration: int = 30
):
    """
    Run a single worker process with specific concurrency configuration.
    This mirrors how production apps run: one worker process with concurrent tasks.
    """
    app.config = Config(
        PROCESSOR_PACKAGES=["tests.acceptance.fixtures.thread_processors"],
        DATABASE_URL=db_url,
        MAX_CONCURRENT_TASKS=max_workers,
        BATCH_SIZE=batch_size,
        POLL_TIMEOUT=duration,  # Will exit after this timeout if no tasks
    )
    asyncio.run(app.process_tasks(channels=("thread-tests",)))


def test_default_config_is_serial(db: Session, db_url: str):
    """
    Test that default configuration (MAX_WORKER_THREADS=1) processes tasks serially.

    This is the critical test that demonstrates the issue: if users don't explicitly
    set MAX_WORKER_THREADS > 1, they get serial execution even though they might
    expect parallel execution.
    """
    # Start a single worker with DEFAULT configuration (MAX_WORKER_THREADS=1)
    proc = Process(
        target=run_single_worker_with_config,
        args=(db_url, 1, 10, 15),  # 1 thread (default), batch size 10
    )
    proc.start()

    # Create 4 tasks that each take 1 second
    task_count = 4
    for i in range(task_count):
        task = slow_task.run(task_num=i, sleep_time=1.0)
        db.add(task)
    db.commit()

    # With serial execution, 4 tasks x 1 second = 4+ seconds
    # With parallel execution (4 threads), would be ~1 second
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
        if delta.total_seconds() > 15:
            raise TimeoutError(
                f"Timeout waiting for all tasks to finish. Only {done_tasks}/{task_count} completed"
            )
        time.sleep(0.5)

    elapsed = (datetime.datetime.now() - begin).total_seconds()

    # Verify all tasks completed
    assert done_tasks == task_count

    # With serial execution, should take at least 4 seconds (4 tasks * 1 second each)
    # Allow some overhead, but it should clearly be serial
    assert elapsed >= 3.5, (
        f"Expected serial execution to take >=3.5s, but took {elapsed:.2f}s (suspiciously fast)"
    )

    # Should definitely take less than 10 seconds (sanity check)
    assert elapsed < 10, f"Serial execution took {elapsed:.2f}s, which is too slow"

    proc.kill()
    proc.join(3)

    print(f"✓ Serial execution confirmed: {task_count} tasks took {elapsed:.2f}s")


def test_explicit_threading_is_concurrent(db: Session, db_url: str):
    """
    Test that explicitly enabling threading (MAX_WORKER_THREADS > 1) provides concurrency.

    This proves that the fix works: when users set MAX_WORKER_THREADS > 1,
    tasks should execute in parallel.
    """
    # Start a single worker with 4 threads
    proc = Process(
        target=run_single_worker_with_config,
        args=(db_url, 4, 10, 15),  # 4 threads, batch size 10
    )
    proc.start()

    # Create 4 tasks that each take 1 second
    task_count = 4
    for i in range(task_count):
        task = slow_task.run(task_num=i, sleep_time=1.0)
        db.add(task)
    db.commit()

    # With 4 threads, 4 tasks should complete in ~1 second (concurrent)
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
        if delta.total_seconds() > 15:
            raise TimeoutError(
                f"Timeout waiting for all tasks to finish. Only {done_tasks}/{task_count} completed"
            )
        time.sleep(0.5)

    elapsed = (datetime.datetime.now() - begin).total_seconds()

    # Verify all tasks completed
    assert done_tasks == task_count

    # With 4 threads running 4 tasks concurrently, should take ~1-2 seconds max
    assert elapsed < 3, (
        f"Expected concurrent execution to take <3s, but took {elapsed:.2f}s (suspiciously slow - no concurrency?)"
    )

    # Should definitely take at least 0.8 seconds (tasks are 1 second each)
    assert elapsed >= 0.8, (
        f"Concurrent execution took {elapsed:.2f}s, which is too fast"
    )

    proc.kill()
    proc.join(3)

    print(f"✓ Concurrent execution confirmed: {task_count} tasks took {elapsed:.2f}s")


def test_realistic_tape_scenario(db: Session, db_url: str):
    """
    Test that mirrors the tape app usage pattern:
    - Single worker process with multiple threads
    - Tasks created via separate connection/session
    - Mixed task durations
    - Continuous arrival of new tasks
    """
    # Start a single worker with 3 threads (matching tape's config)
    proc = Process(
        target=run_single_worker_with_config,
        args=(db_url, 3, 3, 20),  # 3 threads, batch size 3
    )
    proc.start()

    # Give worker time to start
    time.sleep(0.5)

    # Create an initial batch of tasks
    initial_batch = 6
    for i in range(initial_batch):
        task = slow_task.run(task_num=i, sleep_time=0.5)
        db.add(task)
    db.commit()

    # Wait a bit, then add more tasks (simulating dynamic arrival)
    time.sleep(0.3)

    second_batch = 6
    for i in range(initial_batch, initial_batch + second_batch):
        task = slow_task.run(task_num=i, sleep_time=0.5)
        db.add(task)
    db.commit()

    total_tasks = initial_batch + second_batch

    # With 3 threads running 0.5s tasks:
    # - 12 tasks / 3 threads = 4 batches
    # - 4 batches * 0.5s = 2s minimum (if perfectly pipelined)
    # - Realistically 2.5-3.5s with overhead
    begin = datetime.datetime.now()
    while True:
        db.expire_all()
        done_tasks = (
            db.query(models.Task)
            .filter(models.Task.state == models.TaskState.DONE)
            .count()
        )
        if done_tasks == total_tasks:
            break
        delta = datetime.datetime.now() - begin
        if delta.total_seconds() > 15:
            raise TimeoutError(
                f"Timeout waiting for all tasks to finish. Only {done_tasks}/{total_tasks} completed"
            )
        time.sleep(0.5)

    elapsed = (datetime.datetime.now() - begin).total_seconds()

    # Verify all tasks completed
    assert done_tasks == total_tasks

    # With concurrency: should take ~2-4 seconds
    # Without concurrency: would take 12 * 0.5 = 6 seconds
    assert elapsed < 5, (
        f"Expected concurrent execution to take <5s, but took {elapsed:.2f}s (no concurrency?)"
    )
    assert elapsed >= 1.5, f"Execution took {elapsed:.2f}s, which is suspiciously fast"

    proc.kill()
    proc.join(3)

    print(
        f"✓ Realistic tape scenario: {total_tasks} tasks with dynamic arrival took {elapsed:.2f}s"
    )


def test_sync_processors_run_in_worker_threads(db: Session, db_url: str):
    """Sync processors that do not take db run via asyncio.to_thread."""
    proc = Process(
        target=run_single_worker_with_config,
        args=(db_url, 3, 10, 15),
    )
    proc.start()

    task_count = 6
    task_ids = []
    for i in range(task_count):
        task = record_thread_name.run()
        db.add(task)
        db.flush()
        task_ids.append(task.id)
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
        if delta.total_seconds() > 15:
            raise TimeoutError(f"Timeout waiting for tasks")
        time.sleep(0.5)

    completed_tasks = (
        db.query(models.Task)
        .filter(models.Task.id.in_(task_ids))
        .filter(models.Task.state == models.TaskState.DONE)
        .all()
    )

    thread_names = [task.result for task in completed_tasks]
    assert len(thread_names) == task_count
    for name in thread_names:
        assert isinstance(name, str) and name
        assert name != "MainThread"

    proc.kill()
    proc.join(3)


def test_compare_serial_vs_concurrent(db: Session, db_url: str):
    """
    Direct comparison test that runs the same workload with serial and concurrent configs.
    This provides the clearest evidence that threading makes a difference.
    """
    task_count = 8
    task_duration = 0.5  # seconds

    # Test 1: Serial execution (MAX_WORKER_THREADS=1)
    proc_serial = Process(
        target=run_single_worker_with_config,
        args=(db_url, 1, 10, 20),
    )
    proc_serial.start()

    serial_task_ids = []
    for i in range(task_count):
        task = slow_task.run(task_num=i, sleep_time=task_duration)
        db.add(task)
        db.flush()
        serial_task_ids.append(task.id)
    db.commit()

    serial_begin = datetime.datetime.now()
    while True:
        db.expire_all()
        done = (
            db.query(models.Task)
            .filter(models.Task.id.in_(serial_task_ids))
            .filter(models.Task.state == models.TaskState.DONE)
            .count()
        )
        if done == task_count:
            break
        if (datetime.datetime.now() - serial_begin).total_seconds() > 20:
            raise TimeoutError("Serial test timeout")
        time.sleep(0.5)

    serial_elapsed = (datetime.datetime.now() - serial_begin).total_seconds()
    proc_serial.kill()
    proc_serial.join(3)

    # Clean up for next test
    time.sleep(1)

    # Test 2: Concurrent execution (MAX_WORKER_THREADS=4)
    proc_concurrent = Process(
        target=run_single_worker_with_config,
        args=(db_url, 4, 10, 20),
    )
    proc_concurrent.start()

    concurrent_task_ids = []
    for i in range(task_count):
        task = slow_task.run(task_num=i + 1000, sleep_time=task_duration)
        db.add(task)
        db.flush()
        concurrent_task_ids.append(task.id)
    db.commit()

    concurrent_begin = datetime.datetime.now()
    while True:
        db.expire_all()
        done = (
            db.query(models.Task)
            .filter(models.Task.id.in_(concurrent_task_ids))
            .filter(models.Task.state == models.TaskState.DONE)
            .count()
        )
        if done == task_count:
            break
        if (datetime.datetime.now() - concurrent_begin).total_seconds() > 20:
            raise TimeoutError("Concurrent test timeout")
        time.sleep(0.5)

    concurrent_elapsed = (datetime.datetime.now() - concurrent_begin).total_seconds()
    proc_concurrent.kill()
    proc_concurrent.join(3)

    # Compare results
    speedup = serial_elapsed / concurrent_elapsed

    print(f"\n{'=' * 60}")
    print(f"SERIAL (MAX_WORKER_THREADS=1):     {serial_elapsed:.2f}s")
    print(f"CONCURRENT (MAX_WORKER_THREADS=4): {concurrent_elapsed:.2f}s")
    print(f"SPEEDUP:                           {speedup:.2f}x")
    print(f"{'=' * 60}")

    # Serial should take significantly longer
    # Expected: 8 tasks * 0.5s = 4s (serial) vs 2 batches * 0.5s = 1s (4 threads)
    assert serial_elapsed > concurrent_elapsed * 1.5, (
        f"Serial ({serial_elapsed:.2f}s) should be significantly slower than concurrent ({concurrent_elapsed:.2f}s)"
    )

    # Concurrent should show at least 1.5x speedup (conservative, should be closer to 3-4x)
    assert speedup >= 1.5, f"Expected speedup of at least 1.5x, but got {speedup:.2f}x"

    print(f"✓ Concurrency proven: {speedup:.2f}x speedup with threading")
