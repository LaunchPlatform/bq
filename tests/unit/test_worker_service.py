import datetime

import pytest
from sqlalchemy import func
from sqlalchemy.orm import Session

from ..factories import WorkerFactory
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


def test_fetch_dead_workers(
    db: Session, worker_service: WorkerService, worker_factory: WorkerFactory
):
    now = db.scalar(func.now())
    dead_worker0 = worker_factory(last_heartbeat=now - datetime.timedelta(seconds=6))
    dead_worker1 = worker_factory(last_heartbeat=now - datetime.timedelta(seconds=7))
    alive_worker0 = worker_factory(last_heartbeat=now - datetime.timedelta(seconds=4))
    alive_worker1 = worker_factory(last_heartbeat=now - datetime.timedelta(seconds=3))
    alive_worker2 = worker_factory(last_heartbeat=now)
    dead_workers = worker_service.fetch_dead_workers(5).all()
    assert len(dead_workers) == 2
    assert frozenset(worker.id for worker in dead_workers) == frozenset(
        [dead_worker0.id, dead_worker1.id]
    )
    assert dead_worker0.state == models.WorkerState.NO_HEARTBEAT
    assert dead_worker1.state == models.WorkerState.NO_HEARTBEAT
    assert alive_worker0.state == models.WorkerState.RUNNING
    assert alive_worker1.state == models.WorkerState.RUNNING
    assert alive_worker2.state == models.WorkerState.RUNNING
