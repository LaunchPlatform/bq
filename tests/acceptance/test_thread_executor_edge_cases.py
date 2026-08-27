"""Additional acceptance tests for concurrent worker edge cases and robustness."""
import asyncio
import datetime
import time
from multiprocessing import Process

from sqlalchemy.orm import Session

from .fixtures.thread_processors import app
from .fixtures.thread_processors import failing_task
from .fixtures.thread_processors import retry_task
from bq import models
from bq.config import Config


def run_process_cmd_with_threads(
    db_url: str,
    max_workers: int,
    batch_size: int,
    poll_timeout: int = 60,
):
    """Run worker process with concurrent task processing enabled."""
    app.config = Config(
        PROCESSOR_PACKAGES=["tests.acceptance.fixtures.thread_processors"],
        DATABASE_URL=db_url,
        MAX_CONCURRENT_TASKS=max_workers,
        BATCH_SIZE=batch_size,
        POLL_TIMEOUT=poll_timeout,
    )
    asyncio.run(app.process_tasks(channels=("thread-tests",)))


def test_thread_executor_with_failing_tasks(db: Session, db_url: str):
    """Test that thread executor handles task failures gracefully."""
    proc = Process(
        target=run_process_cmd_with_threads,
        args=(db_url, 4, 8),
    )
    proc.start()

    # Create mix of successful and failing tasks
    task_count = 12
    for i in range(task_count):
        # Every 3rd task will fail
        will_fail = (i % 3 == 0)
        task = failing_task.run(task_num=i, should_fail=will_fail)
        db.add(task)
    db.commit()

    # Wait for all tasks to be processed
    begin = datetime.datetime.now()
    expected_done = task_count - (task_count // 3)  # Tasks that won't fail
    expected_failed = task_count // 3  # Tasks that will fail

    while True:
        db.expire_all()
        done_tasks = (
            db.query(models.Task)
            .filter(models.Task.state == models.TaskState.DONE)
            .count()
        )
        failed_tasks = (
            db.query(models.Task)
            .filter(models.Task.state == models.TaskState.FAILED)
            .count()
        )

        if done_tasks + failed_tasks == task_count:
            break

        delta = datetime.datetime.now() - begin
        if delta.total_seconds() > 15:
            raise TimeoutError(
                f"Timeout waiting for tasks. Done: {done_tasks}/{expected_done}, "
                f"Failed: {failed_tasks}/{expected_failed}"
            )
        time.sleep(0.5)

    # Verify correct number of successful and failed tasks
    assert done_tasks == expected_done, f"Expected {expected_done} done tasks, got {done_tasks}"
    assert failed_tasks == expected_failed, f"Expected {expected_failed} failed tasks, got {failed_tasks}"

    # Verify failed tasks have error messages
    failed_task_records = (
        db.query(models.Task)
        .filter(models.Task.state == models.TaskState.FAILED)
        .all()
    )
    for task in failed_task_records:
        assert task.error_message is not None
        assert "Intentional failure" in task.error_message

    proc.kill()
    proc.join(3)


def test_thread_executor_more_threads_than_tasks(db: Session, db_url: str):
    """Test that executor works correctly when threads > tasks."""
    # 8 threads, but only 3 tasks
    proc = Process(
        target=run_process_cmd_with_threads,
        args=(db_url, 8, 10),
    )
    proc.start()

    from .fixtures.thread_processors import slow_task

    task_count = 3
    for i in range(task_count):
        task = slow_task.run(task_num=i, sleep_time=0.2)
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
            raise TimeoutError("Timeout waiting for tasks to finish")
        time.sleep(0.3)

    # All tasks should complete successfully
    assert done_tasks == task_count

    proc.kill()
    proc.join(3)


def test_thread_executor_more_tasks_than_threads(db: Session, db_url: str):
    """Test that executor processes all tasks when tasks > threads."""
    # 2 threads, but 10 tasks
    proc = Process(
        target=run_process_cmd_with_threads,
        args=(db_url, 2, 5),
    )
    proc.start()

    from .fixtures.thread_processors import slow_task

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
        if delta.total_seconds() > 15:
            raise TimeoutError(
                f"Timeout waiting for tasks. Done: {done_tasks}/{task_count}"
            )
        time.sleep(0.3)

    # All tasks should complete successfully
    assert done_tasks == task_count

    # Verify all results are correct
    completed_tasks = (
        db.query(models.Task)
        .filter(models.Task.state == models.TaskState.DONE)
        .all()
    )
    for task in completed_tasks:
        expected_result = task.kwargs["task_num"] * 2
        assert task.result == expected_result

    proc.kill()
    proc.join(3)


def test_thread_executor_with_retry_policy(db: Session, db_url: str):
    """Test that retry policies work correctly with thread executor."""
    # Use a short poll timeout so worker picks up rescheduled tasks quickly
    proc = Process(
        target=run_process_cmd_with_threads,
        args=(db_url, 4, 8, 2),  # poll_timeout=2 seconds
    )
    proc.start()

    # Create tasks that will fail initially but succeed on retry
    task_count = 4
    for i in range(task_count):
        task = retry_task.run(task_num=i, max_attempts=2)
        db.add(task)
    db.commit()

    # Tasks should fail first, then be retried and succeed
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
            pending = db.query(models.Task).filter(
                models.Task.state == models.TaskState.PENDING
            ).count()
            failed = db.query(models.Task).filter(
                models.Task.state == models.TaskState.FAILED
            ).count()
            raise TimeoutError(
                f"Timeout waiting for retried tasks. "
                f"Done: {done_tasks}/{task_count}, Pending: {pending}, Failed: {failed}"
            )
        time.sleep(0.5)

    # All tasks should eventually succeed after retry
    assert done_tasks == task_count

    proc.kill()
    proc.join(3)


def test_thread_executor_task_state_transitions(db: Session, db_url: str):
    """Test that task state transitions are correct with thread executor."""
    proc = Process(
        target=run_process_cmd_with_threads,
        args=(db_url, 2, 4),
    )
    proc.start()

    from .fixtures.thread_processors import slow_task

    # Create tasks and track their states
    task_count = 4
    task_ids = []
    for i in range(task_count):
        task = slow_task.run(task_num=i, sleep_time=0.2)
        db.add(task)
        db.flush()
        task_ids.append(task.id)
    db.commit()

    # All tasks should start in PENDING state
    pending_count = (
        db.query(models.Task)
        .filter(models.Task.id.in_(task_ids))
        .filter(models.Task.state == models.TaskState.PENDING)
        .count()
    )
    assert pending_count == task_count

    # Wait for tasks to move to PROCESSING
    time.sleep(0.5)
    db.expire_all()

    # Some tasks should be in PROCESSING or DONE
    processing_or_done = (
        db.query(models.Task)
        .filter(models.Task.id.in_(task_ids))
        .filter(
            models.Task.state.in_([models.TaskState.PROCESSING, models.TaskState.DONE])
        )
        .count()
    )
    assert processing_or_done > 0, "Tasks should transition to PROCESSING"

    # Wait for all to complete
    begin = datetime.datetime.now()
    while True:
        db.expire_all()
        done_count = (
            db.query(models.Task)
            .filter(models.Task.id.in_(task_ids))
            .filter(models.Task.state == models.TaskState.DONE)
            .count()
        )
        if done_count == task_count:
            break
        delta = datetime.datetime.now() - begin
        if delta.total_seconds() > 10:
            raise TimeoutError("Timeout waiting for tasks to complete")
        time.sleep(0.3)

    # All tasks should be in DONE state
    assert done_count == task_count

    # No tasks should be in PROCESSING (all cleaned up)
    processing_count = (
        db.query(models.Task)
        .filter(models.Task.id.in_(task_ids))
        .filter(models.Task.state == models.TaskState.PROCESSING)
        .count()
    )
    assert processing_count == 0, "All tasks should transition from PROCESSING to DONE"

    proc.kill()
    proc.join(3)
