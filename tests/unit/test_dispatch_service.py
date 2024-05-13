import pytest
from sqlalchemy.orm import Session

from bq import models
from bq.services.dispatch import DispatchService


@pytest.fixture
def dispatch_service() -> DispatchService:
    return DispatchService()


def test_fetch_empty(
    db: Session, dispatch_service: DispatchService, worker: models.Worker
):
    assert not dispatch_service.fetch("test", worker=worker)


def test_fetch(
    db: Session,
    dispatch_service: DispatchService,
    worker: models.Worker,
    task: models.Task,
):
    assert task.state == models.TaskState.PENDING
    tasks = list(dispatch_service.fetch(task.channel, worker=worker))
    assert len(tasks) == 1
    returned_task = tasks[0]
    assert returned_task.state == models.TaskState.PROCESSING
    assert returned_task.worker == worker
