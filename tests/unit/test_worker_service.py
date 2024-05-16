import pytest
from sqlalchemy import func
from sqlalchemy.orm import Session

from bq import models
from bq.services.worker import WorkerService


@pytest.fixture
def worker_service(db: Session) -> WorkerService:
    return WorkerService(db)


def test_update_heartbeat(
    db: Session, worker_service: WorkerService, worker: models.Worker
):
    now = db.scalar(func.now())
    assert worker.last_heartbeat != now
    worker_service.update_heartbeat(worker)
    db.commit()
    assert worker.last_heartbeat == now
