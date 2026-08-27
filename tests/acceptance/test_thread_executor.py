import asyncio
import datetime
import threading
import time
from multiprocessing import Process

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from .fixtures.thread_processors import app
from .fixtures.thread_processors import slow_task
from .fixtures.thread_processors import concurrent_task
from .fixtures.thread_processors import timed_task
from bq import models
from bq.config import Config


def run_process_cmd_with_threads(db_url: str, max_workers: int, batch_size: int):
    """Run worker process with concurrent task processing enabled."""
    app.config = Config(
        PROCESSOR_PACKAGES=["tests.acceptance.fixtures.thread_processors"],
        DATABASE_URL=db_url,
        MAX_CONCURRENT_TASKS=max_workers,
        BATCH_SIZE=batch_size,
    )
    asyncio.run(app.process_tasks(channels=("thread-tests",)))


def test_thread_executor_with_multiple_threads(db: Session, db_url: str):
    """Test that thread executor processes tasks concurrently within a single worker."""
    # Use a single worker process with 4 threads
    proc = Process(
        target=run_process_cmd_with_threads,
        args=(db_url, 4, 10),  # 4 threads, batch size 10
    )
    proc.start()

    # Create 16 tasks (each takes 0.2 seconds)
    task_count = 16
    for i in range(task_count):
        task = slow_task.run(task_num=i, sleep_time=0.2)
        db.add(task)
    db.commit()

    # With 4 threads, 16 tasks should complete in ~1 second
    # (16 tasks / 4 threads = 4 batches, 4 * 0.2s = 0.8s + overhead)
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

    # Verify results are correct
    completed_tasks = (
        db.query(models.Task).filter(models.Task.state == models.TaskState.DONE).all()
    )
    for task in completed_tasks:
        # Result should be task_num * 2
        expected_result = task.kwargs["task_num"] * 2
        assert task.result == expected_result, f"Task {task.id} has incorrect result"

    proc.kill()
    proc.join(3)


def test_thread_executor_session_isolation(db: Session, db_url: str):
    """Test that each thread has its own database session without conflicts."""
    # Use a single worker process with 4 threads
    proc = Process(
        target=run_process_cmd_with_threads,
        args=(db_url, 4, 8),  # 4 threads, batch size 8
    )
    proc.start()

    # Create 12 tasks that will be processed concurrently
    task_count = 12
    for i in range(task_count):
        task = concurrent_task.run(value=i)
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
        if delta.total_seconds() > 15:
            # Check for failed tasks
            failed_tasks = (
                db.query(models.Task)
                .filter(models.Task.state == models.TaskState.FAILED)
                .count()
            )
            raise TimeoutError(
                f"Timeout waiting for all tasks to finish. "
                f"Done: {done_tasks}/{task_count}, Failed: {failed_tasks}"
            )
        time.sleep(0.5)

    # Verify all tasks completed successfully (no session conflicts)
    failed_tasks = (
        db.query(models.Task)
        .filter(models.Task.state == models.TaskState.FAILED)
        .count()
    )
    assert failed_tasks == 0, f"Found {failed_tasks} failed tasks due to session conflicts"
    assert done_tasks == task_count, f"Only {done_tasks}/{task_count} tasks completed"

    proc.kill()
    proc.join(3)


def test_sequential_processing_backward_compatibility(db: Session, db_url: str):
    """Test that MAX_WORKER_THREADS=1 maintains sequential processing behavior."""
    # Use a single worker process with sequential processing (MAX_WORKER_THREADS=1)
    proc = Process(
        target=run_process_cmd_with_threads,
        args=(db_url, 1, 5),  # 1 thread (sequential), batch size 5
    )
    proc.start()

    task_count = 10
    for i in range(task_count):
        task = slow_task.run(task_num=i, sleep_time=0.1)
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
        if delta.total_seconds() > 10:
            raise TimeoutError("Timeout waiting for all tasks to finish")
        time.sleep(0.2)

    # Verify all tasks completed
    assert done_tasks == task_count

    proc.kill()
    proc.join(3)


def test_tasks_arriving_during_processing_run_concurrently(db: Session, db_url: str):
    """Test that tasks arriving while others are processing get picked up immediately.

    This simulates the scenario of two browser tabs submitting tasks - the second task
    should start processing as soon as it's created, not wait for the first to complete.
    """
    # Worker with 3 threads, so we have capacity for concurrent tasks
    proc = Process(
        target=run_process_cmd_with_threads,
        args=(db_url, 3, 5),  # 3 threads, batch size 5
    )
    proc.start()

    # Give the worker time to start and begin polling
    time.sleep(0.5)

    # Submit first long-running task (simulates first browser tab)
    task1 = timed_task.run(task_num=1, sleep_time=1.5)
    db.add(task1)
    db.commit()
    task1_id = task1.id

    # Wait a bit for task1 to start processing
    time.sleep(0.3)

    # Submit second long-running task (simulates second browser tab)
    task2 = timed_task.run(task_num=2, sleep_time=1.5)
    db.add(task2)
    db.commit()
    task2_id = task2.id

    # Wait for both tasks to complete
    begin = datetime.datetime.now()
    while True:
        db.expire_all()

        t1 = db.query(models.Task).filter(models.Task.id == task1_id).one()
        t2 = db.query(models.Task).filter(models.Task.id == task2_id).one()

        if t1.state == models.TaskState.DONE and t2.state == models.TaskState.DONE:
            break

        delta = datetime.datetime.now() - begin
        if delta.total_seconds() > 10:
            raise TimeoutError(
                f"Timeout waiting for tasks. Task1 state: {t1.state}, Task2 state: {t2.state}"
            )
        time.sleep(0.1)

    # Get the recorded start/end times from task results
    t1_result = t1.result
    t2_result = t2.result

    t1_start, t1_end = t1_result["start"], t1_result["end"]
    t2_start, t2_end = t2_result["start"], t2_result["end"]

    # Check for time overlap - if tasks ran concurrently, their execution windows should overlap
    # Overlap exists if: t1_start < t2_end AND t2_start < t1_end
    has_overlap = (t1_start < t2_end) and (t2_start < t1_end)

    # Calculate overlap duration
    overlap_start = max(t1_start, t2_start)
    overlap_end = min(t1_end, t2_end)
    overlap_duration = max(0, overlap_end - overlap_start)

    assert has_overlap, (
        f"Tasks did NOT run concurrently! No time overlap detected.\n"
        f"Task1: start={t1_start:.3f}, end={t1_end:.3f}\n"
        f"Task2: start={t2_start:.3f}, end={t2_end:.3f}"
    )

    # With 1.5s tasks and 0.3s delay between submissions, we expect ~1.2s overlap
    assert overlap_duration > 0.5, (
        f"Tasks had minimal overlap ({overlap_duration:.2f}s), expected >0.5s for true concurrency.\n"
        f"Task1: start={t1_start:.3f}, end={t1_end:.3f}\n"
        f"Task2: start={t2_start:.3f}, end={t2_end:.3f}"
    )

    proc.kill()
    proc.join(3)


def test_concurrent_execution_with_timed_tasks(db: Session, db_url: str):
    """Test that multiple tasks created at the same time actually execute in parallel.

    This creates several tasks simultaneously and verifies their execution times overlap,
    proving the thread pool is working correctly.
    """
    proc = Process(
        target=run_process_cmd_with_threads,
        args=(db_url, 4, 10),  # 4 threads, batch size 10
    )
    proc.start()

    # Create 4 tasks that each take 1 second
    task_count = 4
    task_ids = []
    for i in range(task_count):
        task = timed_task.run(task_num=i, sleep_time=1.0)
        db.add(task)
        db.flush()
        task_ids.append(task.id)
    db.commit()

    # Wait for all tasks to complete
    begin = datetime.datetime.now()
    while True:
        db.expire_all()
        done_tasks = (
            db.query(models.Task)
            .filter(models.Task.id.in_(task_ids))
            .filter(models.Task.state == models.TaskState.DONE)
            .count()
        )
        if done_tasks == task_count:
            break
        delta = datetime.datetime.now() - begin
        if delta.total_seconds() > 15:
            raise TimeoutError(f"Timeout. Only {done_tasks}/{task_count} completed")
        time.sleep(0.2)

    # Get all task results
    tasks = db.query(models.Task).filter(models.Task.id.in_(task_ids)).all()
    timings = [t.result for t in tasks]

    # Find the overall time span
    earliest_start = min(t["start"] for t in timings)
    latest_end = max(t["end"] for t in timings)
    total_wall_time = latest_end - earliest_start

    # If tasks ran sequentially, 4 tasks * 1s each = 4s minimum
    # If tasks ran concurrently (4 threads), ~1s total
    # Allow some overhead, but should be well under 2s for true concurrency
    assert total_wall_time < 2.0, (
        f"Tasks appear to have run sequentially! "
        f"Total wall time: {total_wall_time:.2f}s (expected <2s for 4 concurrent 1s tasks)"
    )

    # Verify there was actual overlap between tasks
    # Count how many task pairs have overlapping execution
    overlap_count = 0
    for i in range(len(timings)):
        for j in range(i + 1, len(timings)):
            t1, t2 = timings[i], timings[j]
            if (t1["start"] < t2["end"]) and (t2["start"] < t1["end"]):
                overlap_count += 1

    # With 4 concurrent tasks, we expect all 6 pairs to overlap (4 choose 2 = 6)
    assert overlap_count >= 3, (
        f"Expected at least 3 overlapping task pairs, got {overlap_count}. "
        f"Timings: {timings}"
    )

    proc.kill()
    proc.join(3)


def test_tasks_from_separate_connections_run_concurrently(db: Session, db_url: str):
    """Test concurrent execution when tasks are created from separate DB connections.

    This simulates the real-world scenario where tasks are created by different
    web request handlers (like two browser tabs), each with their own DB connection.
    This is the scenario that was failing in production.
    """
    proc = Process(
        target=run_process_cmd_with_threads,
        args=(db_url, 3, 5),  # 3 threads, batch size 5
    )
    proc.start()

    # Give worker time to start
    time.sleep(0.5)

    task_ids = []
    task_ids_lock = threading.Lock()

    def create_task_in_separate_connection(task_num: int, sleep_time: float, delay: float):
        """Create a task using a completely separate database connection."""
        time.sleep(delay)  # Stagger task creation

        # Create a new engine and session - simulates a separate web request
        engine = create_engine(db_url)
        session = Session(bind=engine)
        try:
            task = timed_task.run(task_num=task_num, sleep_time=sleep_time)
            session.add(task)
            session.commit()
            with task_ids_lock:
                task_ids.append(task.id)
        finally:
            session.close()
            engine.dispose()

    # Create tasks from separate threads with separate connections
    # Task 1: starts immediately, runs for 2 seconds
    # Task 2: starts 0.5s later, runs for 2 seconds
    threads = [
        threading.Thread(target=create_task_in_separate_connection, args=(1, 2.0, 0)),
        threading.Thread(target=create_task_in_separate_connection, args=(2, 2.0, 0.5)),
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Wait for both tasks to complete
    begin = datetime.datetime.now()
    while True:
        db.expire_all()
        with task_ids_lock:
            current_ids = list(task_ids)

        if len(current_ids) < 2:
            time.sleep(0.1)
            continue

        done_tasks = (
            db.query(models.Task)
            .filter(models.Task.id.in_(current_ids))
            .filter(models.Task.state == models.TaskState.DONE)
            .all()
        )

        if len(done_tasks) == 2:
            break

        delta = datetime.datetime.now() - begin
        if delta.total_seconds() > 15:
            states = {
                t.id: t.state
                for t in db.query(models.Task).filter(models.Task.id.in_(current_ids)).all()
            }
            raise TimeoutError(f"Timeout waiting for tasks. States: {states}")
        time.sleep(0.1)

    # Get timing results
    t1 = db.query(models.Task).filter(models.Task.id == task_ids[0]).one()
    t2 = db.query(models.Task).filter(models.Task.id == task_ids[1]).one()

    t1_result = t1.result
    t2_result = t2.result

    t1_start, t1_end = t1_result["start"], t1_result["end"]
    t2_start, t2_end = t2_result["start"], t2_result["end"]

    # Check for overlap
    has_overlap = (t1_start < t2_end) and (t2_start < t1_end)
    overlap_start = max(t1_start, t2_start)
    overlap_end = min(t1_end, t2_end)
    overlap_duration = max(0, overlap_end - overlap_start)

    # If sequential: task2 starts after task1 ends, total time ~4s
    # If concurrent: tasks overlap by ~1.5s, total time ~2.5s
    total_time = max(t1_end, t2_end) - min(t1_start, t2_start)

    assert has_overlap, (
        f"Tasks from separate connections did NOT run concurrently!\n"
        f"Task1: start={t1_start:.3f}, end={t1_end:.3f}\n"
        f"Task2: start={t2_start:.3f}, end={t2_end:.3f}\n"
        f"Total time: {total_time:.2f}s (expected ~2.5s if concurrent, ~4s if sequential)"
    )

    assert overlap_duration > 1.0, (
        f"Tasks had insufficient overlap ({overlap_duration:.2f}s).\n"
        f"Task1: start={t1_start:.3f}, end={t1_end:.3f}\n"
        f"Task2: start={t2_start:.3f}, end={t2_end:.3f}\n"
        f"Expected >1s overlap for true concurrent execution."
    )

    proc.kill()
    proc.join(3)


def test_staggered_task_arrival_proves_concurrency(db: Session, db_url: str):
    """Definitive test that proves whether late-arriving tasks run concurrently.

    This test uses long sleep times (5 seconds) and staggered task submission
    to make the difference between concurrent and sequential execution
    unmistakable:

    - If CONCURRENT: Task1 (5s) + Task2 (5s, arrives 1s later) = ~6s total
    - If SEQUENTIAL: Task1 finishes, then Task2 runs = ~10s total

    This test will FAIL if the second task waits for the first to complete.
    """
    # Worker with 3 threads - plenty of capacity for 2 tasks
    proc = Process(
        target=run_process_cmd_with_threads,
        args=(db_url, 3, 5),
    )
    proc.start()

    # Give worker time to start and enter its processing loop
    time.sleep(1.0)

    task_ids = []
    task_ids_lock = threading.Lock()

    def create_task_in_separate_connection(task_num: int, sleep_time: float, delay: float):
        """Create a task using a completely separate database connection."""
        time.sleep(delay)

        engine = create_engine(db_url)
        session = Session(bind=engine)
        try:
            task = timed_task.run(task_num=task_num, sleep_time=sleep_time)
            session.add(task)
            session.commit()
            print(f"Task {task_num} created at {time.time():.3f}, id={task.id}")
            with task_ids_lock:
                task_ids.append((task_num, task.id))
        finally:
            session.close()
            engine.dispose()

    # Create tasks from separate threads (simulating separate browser tabs)
    # Task 1: created immediately, runs for 5 seconds
    # Task 2: created 1 second later, runs for 5 seconds
    wall_clock_start = time.time()

    threads = [
        threading.Thread(target=create_task_in_separate_connection, args=(1, 5.0, 0)),
        threading.Thread(target=create_task_in_separate_connection, args=(2, 5.0, 1.0)),
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Wait for both tasks to complete
    begin = datetime.datetime.now()
    while True:
        db.expire_all()
        with task_ids_lock:
            current_ids = [tid for _, tid in task_ids]

        if len(current_ids) < 2:
            time.sleep(0.1)
            continue

        done_tasks = (
            db.query(models.Task)
            .filter(models.Task.id.in_(current_ids))
            .filter(models.Task.state == models.TaskState.DONE)
            .all()
        )

        if len(done_tasks) == 2:
            break

        # Check for PROCESSING state to see what's happening
        processing_tasks = (
            db.query(models.Task)
            .filter(models.Task.id.in_(current_ids))
            .filter(models.Task.state == models.TaskState.PROCESSING)
            .count()
        )

        delta = datetime.datetime.now() - begin
        if delta.total_seconds() > 25:
            states = {
                t.id: t.state
                for t in db.query(models.Task).filter(models.Task.id.in_(current_ids)).all()
            }
            raise TimeoutError(
                f"Timeout waiting for tasks. States: {states}, "
                f"Currently processing: {processing_tasks}"
            )
        time.sleep(0.2)

    wall_clock_end = time.time()
    total_wall_time = wall_clock_end - wall_clock_start

    # Get timing results from task results
    with task_ids_lock:
        task1_id = next(tid for num, tid in task_ids if num == 1)
        task2_id = next(tid for num, tid in task_ids if num == 2)

    t1 = db.query(models.Task).filter(models.Task.id == task1_id).one()
    t2 = db.query(models.Task).filter(models.Task.id == task2_id).one()

    t1_result = t1.result
    t2_result = t2.result

    t1_start, t1_end = t1_result["start"], t1_result["end"]
    t2_start, t2_end = t2_result["start"], t2_result["end"]

    # Calculate overlap
    has_overlap = (t1_start < t2_end) and (t2_start < t1_end)
    overlap_start = max(t1_start, t2_start)
    overlap_end = min(t1_end, t2_end)
    overlap_duration = max(0, overlap_end - overlap_start)

    task_time_span = max(t1_end, t2_end) - min(t1_start, t2_start)

    print(f"\n=== Concurrency Test Results ===")
    print(f"Task1: start={t1_start:.3f}, end={t1_end:.3f}, duration={t1_end-t1_start:.2f}s")
    print(f"Task2: start={t2_start:.3f}, end={t2_end:.3f}, duration={t2_end-t2_start:.2f}s")
    print(f"Overlap: {overlap_duration:.2f}s")
    print(f"Task time span: {task_time_span:.2f}s")
    print(f"Total wall time: {total_wall_time:.2f}s")
    print(f"Has overlap: {has_overlap}")

    # THE KEY ASSERTION: If concurrent, total should be ~6s. If sequential, ~10s.
    # We use 8s as the threshold - anything above this indicates sequential execution.
    assert total_wall_time < 8.0, (
        f"CONCURRENCY FAILURE: Tasks ran SEQUENTIALLY!\n"
        f"Total wall time: {total_wall_time:.2f}s (expected <8s for concurrent, ~10s for sequential)\n"
        f"Task1: start={t1_start:.3f}, end={t1_end:.3f}\n"
        f"Task2: start={t2_start:.3f}, end={t2_end:.3f}\n"
        f"This proves the second task waited for the first to complete."
    )

    # Also verify there was meaningful overlap
    assert has_overlap, (
        f"Tasks had no execution overlap - they ran sequentially!\n"
        f"Task1: start={t1_start:.3f}, end={t1_end:.3f}\n"
        f"Task2: start={t2_start:.3f}, end={t2_end:.3f}"
    )

    # Overlap should be at least 3 seconds (5s task - 1s stagger - 1s margin)
    assert overlap_duration > 3.0, (
        f"Tasks had insufficient overlap ({overlap_duration:.2f}s).\n"
        f"Expected >3s overlap for true concurrent execution.\n"
        f"Task1: start={t1_start:.3f}, end={t1_end:.3f}\n"
        f"Task2: start={t2_start:.3f}, end={t2_end:.3f}"
    )

    proc.kill()
    proc.join(3)
