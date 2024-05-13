from .. import models
from ..db.session import Session


class DispatchService:
    def fetch(
        self, channel: str, worker: models.Worker, limit: int = 1
    ) -> list[models.Task]:
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
        result = session.execute(
            models.Task.__table__.update()
            .where(models.Task.id.in_(task_subquery))
            .values(
                state=models.TaskState.PROCESSING,
                worker=worker,
            )
            .returning(models.Task)
        )
        return list(result)
