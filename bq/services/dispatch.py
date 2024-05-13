import typing
import uuid

from sqlalchemy.orm import Query

from .. import models
from ..db.session import Session


class DispatchService:
    def __init__(self, session_cls: typing.Type[Session] = Session):
        self.session_cls = session_cls

    def make_task_query(self, predicate: typing.Any, limit: int = 1) -> Query:
        session = self.session_cls()
        return (
            session.query(models.Task.id)
            .filter(predicate)
            .filter(models.Task.state == models.TaskState.PENDING)
            .order_by(models.Task.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

    def make_update_query(self, task_query: typing.Any, worker_id: uuid.UUID):
        return (
            models.Task.__table__.update()
            .where(models.Task.id.in_(task_query))
            .values(
                state=models.TaskState.PROCESSING,
                worker_id=worker_id,
            )
            .returning(models.Task.id)
        )

    def dispatch(
        self, predicate: typing.Any, worker: models.Worker, limit: int = 1
    ) -> Query:
        session = self.session_cls
        task_query = self.make_task_query(predicate, limit=limit)
        task_subquery = task_query.subquery("locked_tasks")
        task_ids = [
            item[0]
            for item in session.execute(
                self.make_update_query(task_subquery, worker_id=worker.id)
            )
        ]
        # TODO: ideally returning with (models.Task) should return the whole model, but SQLAlchemy is returning
        #       it columns in rows. We can save a round trip if we can find out how to solve this
        return session.query(models.Task).filter(models.Task.id.in_(task_ids))
