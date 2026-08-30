import datetime
import threading
import time

import bq
from bq.processors.retry_policies import DelayRetry

app = bq.BeanQueue()


@app.processor(channel="thread-tests")
def slow_task(task: bq.Task, task_num: int, sleep_time: float):
    """A slow task that simulates I/O-bound work."""
    time.sleep(sleep_time)
    return task_num * 2


@app.processor(channel="thread-tests")
def timed_task(task: bq.Task, task_num: int, sleep_time: float):
    """A task that records its start and end times to prove concurrent execution."""
    start_time = time.time()
    time.sleep(sleep_time)
    end_time = time.time()
    return {"task_num": task_num, "start": start_time, "end": end_time}


@app.processor(channel="thread-tests")
def concurrent_task(task: bq.Task, value: int):
    """A task to test concurrent execution and session isolation."""
    # Simulate some work
    time.sleep(0.1)
    result = value**2
    return result


@app.processor(channel="thread-tests")
def failing_task(task: bq.Task, task_num: int, should_fail: bool):
    """A task that fails conditionally to test error handling."""
    time.sleep(0.1)
    if should_fail:
        raise ValueError(f"Intentional failure for task {task_num}")
    return task_num * 2


# Retry policy: retry once after 0.5 seconds
retry_policy = DelayRetry(delay=datetime.timedelta(seconds=0.5))


@app.processor(channel="thread-tests", retry_policy=retry_policy)
def retry_task(task: bq.Task, task_num: int, max_attempts: int):
    """A task that fails initially but succeeds on retry."""
    time.sleep(0.1)

    # Count attempts by checking if this is first attempt (no scheduled_at)
    # or a retry (has scheduled_at set)
    if task.scheduled_at is None:
        # First attempt - always fail
        raise ValueError(f"First attempt fails for task {task_num}")
    else:
        # Retry attempt - succeed
        return task_num * 3


@app.processor(channel="thread-tests")
def record_thread_name() -> str:
    """Records the current thread name for debugging and testing."""
    thread_name = threading.current_thread().name
    return thread_name
