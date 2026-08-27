import datetime
import typing

from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from .. import models


class WorkerService:
    def __init__(
        self,
        session: AsyncSession,
        task_model: typing.Type = models.Task,
        worker_model: typing.Type = models.Worker,
    ):
        self.session = session
        self.task_model: typing.Type[models.Task] = task_model
        self.worker_model: typing.Type[models.Worker] = worker_model

    async def get_worker(self, id: typing.Any) -> typing.Any:
        return await self.session.get(self.worker_model, id)

    def make_worker(self, name: str, channels: tuple[str, ...]):
        return self.worker_model(name=name, channels=channels)

    def update_heartbeat(self, worker: models.Worker):
        worker.last_heartbeat = func.now()
        self.session.add(worker)

    def make_dead_worker_query(self, timeout: int, limit: int = 5):
        return (
            select(self.worker_model.id)
            .where(
                self.worker_model.last_heartbeat
                < (func.now() - datetime.timedelta(seconds=timeout))
            )
            .where(self.worker_model.state == models.WorkerState.RUNNING)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

    def make_update_dead_worker_query(self, worker_query: typing.Any):
        return (
            update(self.worker_model)
            .where(self.worker_model.id.in_(worker_query))
            .values(
                state=models.WorkerState.NO_HEARTBEAT,
            )
            .returning(self.worker_model.id)
        )

    async def fetch_dead_workers(
        self, timeout: int, limit: int = 5
    ) -> list[models.Worker]:
        dead_worker_query = self.make_dead_worker_query(timeout=timeout, limit=limit)
        dead_worker_subquery = dead_worker_query.scalar_subquery()
        result = await self.session.execute(
            self.make_update_dead_worker_query(dead_worker_subquery)
        )
        worker_ids = [item[0] for item in result]
        if not worker_ids:
            return []
        workers = await self.session.scalars(
            select(self.worker_model).where(self.worker_model.id.in_(worker_ids))
        )
        return list(workers.all())

    def make_update_tasks_query(self, worker_query: typing.Any):
        return (
            update(self.task_model)
            .where(self.task_model.worker_id.in_(worker_query))
            .where(self.task_model.state == models.TaskState.PROCESSING)
            .values(
                state=models.TaskState.PENDING,
                worker_id=None,
            )
        )

    async def reschedule_dead_tasks(self, worker_query: typing.Any) -> int:
        update_dead_task_query = self.make_update_tasks_query(worker_query=worker_query)
        res = await self.session.execute(update_dead_task_query)
        return res.rowcount
