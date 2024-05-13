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
