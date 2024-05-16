import datetime
import typing

from sqlalchemy import func
from sqlalchemy.orm import Query
from sqlalchemy.orm import Session

from .. import models


class WorkerService:
    def __init__(self, session: Session):
        self.session = session

    def update_heartbeat(self, worker: models.Worker):
        worker.last_heartbeat = func.now()
        self.session.add(worker)

    def make_dead_worker_query(self, timeout: int, limit: int = 5) -> Query:
        return (
            self.session.query(models.Worker.id)
            .filter(
                models.Worker.last_heartbeat
                < (func.now() - datetime.timedelta(seconds=timeout))
            )
            .filter(models.Worker.state == models.WorkerState.RUNNING)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

    def make_update_dead_worker_query(self, worker_query: typing.Any):
        return (
            models.Worker.__table__.update()
            .where(models.Worker.id.in_(worker_query))
            .values(
                state=models.WorkerState.NO_HEARTBEAT,
            )
            .returning(models.Worker.id)
        )

    def fetch_dead_workers(self, timeout: int, limit: int = 5) -> Query:
        dead_worker_query = self.make_dead_worker_query(timeout=timeout, limit=limit)
        dead_worker_subquery = dead_worker_query.scalar_subquery()
        worker_ids = [
            item[0]
            for item in self.session.execute(
                self.make_update_dead_worker_query(dead_worker_subquery)
            )
        ]
        # TODO: ideally returning with (models.Task) should return the whole model, but SQLAlchemy is returning
        #       it columns in rows. We can save a round trip if we can find out how to solve this
        return self.session.query(models.Worker).filter(
            models.Worker.id.in_(worker_ids)
        )

    def make_update_tasks_query(self, worker_query: typing.Any):
        return (
            models.Task.__table__.update()
            .where(models.Task.worker_id.in_(worker_query))
            .where(models.Task.state == models.TaskState.PROCESSING)
            .values(
                state=models.TaskState.PENDING,
            )
        )

    def reschedule_dead_tasks(self, worker_query: typing.Any) -> int:
        update_dead_task_query = self.make_update_tasks_query(worker_query=worker_query)
        res = self.session.execute(update_dead_task_query)
        return res.rowcount
