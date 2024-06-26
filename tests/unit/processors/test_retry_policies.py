import datetime

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.orm import Session

from ...factories import EventFactory
from bq import models
from bq.processors.retry_policies import DelayRetry
from bq.processors.retry_policies import ExponentialBackoffRetry
from bq.processors.retry_policies import LimitAttempt


@pytest.mark.parametrize("failure_count", [0, 1, 5, 10])
def test_delay_policy(
    db: Session, event_factory: EventFactory, task: models.Task, failure_count: int
):
    for _ in range(failure_count):
        event_factory(task=task, type=models.EventType.FAILED_RETRY_SCHEDULED)
    delay = DelayRetry(delay=datetime.timedelta(seconds=5))
    scheduled_at = delay(task)
    expected = db.scalar(select(func.now() + datetime.timedelta(seconds=5)))
    actual = db.scalar(select(scheduled_at))
    assert actual == expected


@pytest.mark.parametrize(
    "failure_count, expected_delay",
    [
        (0, 32),
        (1, 128),
        (5, 32768),
        (10, 33554432),
    ],
)
def test_exponential_backoff(
    db: Session,
    event_factory: EventFactory,
    task: models.Task,
    failure_count: int,
    expected_delay: int,
):
    for _ in range(failure_count):
        event_factory(task=task, type=models.EventType.FAILED_RETRY_SCHEDULED)
    backoff = ExponentialBackoffRetry(base=2, exponent_offset=3, exponent_scalar=2)
    scheduled_at = backoff(task)
    expected = db.scalar(
        select(func.now() + datetime.timedelta(seconds=expected_delay))
    )
    actual = db.scalar(select(scheduled_at))
    assert actual == expected


@pytest.mark.parametrize(
    "failure_count, expected_retry",
    [
        (0, True),
        (1, True),
        (5, False),
        (6, False),
        (7, False),
    ],
)
def test_limit_attempt(
    db: Session,
    event_factory: EventFactory,
    task: models.Task,
    failure_count: int,
    expected_retry: bool,
):
    for _ in range(failure_count):
        event_factory(task=task, type=models.EventType.FAILED_RETRY_SCHEDULED)
    policy = LimitAttempt(6, DelayRetry(delay=datetime.timedelta(seconds=5)))
    scheduled_at = policy(task)
    if expected_retry:
        assert scheduled_at is not None
    else:
        assert scheduled_at is None
