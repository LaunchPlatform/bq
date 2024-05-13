from sqlalchemy.orm import Query

from .. import models
from ..db.session import Session


class DispatchService:
    def fetch(self, channel: str, worker: models.Worker, limit: int = 1) -> Query:
        session = Session()
        task_query = (
            session.query(models.Task.id)
            .filter(models.Task.channel == channel)
            .filter(models.Task.state == models.TaskState.PENDING)
            .order_by(models.Task.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        task_subquery = task_query.subquery("locked_tasks")
        task_ids = [
            item[0]
            for item in session.execute(
                models.Task.__table__.update()
                .where(models.Task.id.in_(task_subquery))
                .values(
                    state=models.TaskState.PROCESSING,
                    worker_id=worker.id,
                )
                .returning(models.Task.id)
            )
        ]
        # TODO: ideally returning with (models.Task) should return the whole model, but SQLAlchemy is returning
        #       it columns in rows. We can save a round trip if we can find out how to solve this
        return session.query(models.Task).filter(models.Task.id.in_(task_ids))
