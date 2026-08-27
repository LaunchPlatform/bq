import dataclasses
import typing
import uuid

import psycopg
from sqlalchemy import func
from sqlalchemy import null
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

from .. import models


@dataclasses.dataclass(frozen=True)
class Notification:
    pid: int
    channel: str
    payload: typing.Optional[str] = None


class DispatchService:
    def __init__(self, session: AsyncSession, task_model: typing.Type = models.Task):
        self.session = session
        self.task_model: typing.Type[models.Task] = task_model
        self._listen_conn: psycopg.AsyncConnection | None = None

    def make_task_query(
        self,
        channels: typing.Sequence[str],
        limit: int = 1,
        now: typing.Any = func.now(),
    ) -> Select:
        return (
            select(self.task_model.id)
            .where(self.task_model.channel.in_(channels))
            .where(self.task_model.state == models.TaskState.PENDING)
            .where(
                or_(
                    self.task_model.scheduled_at.is_(null()),
                    now >= self.task_model.scheduled_at,
                )
            )
            .order_by(self.task_model.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

    def make_update_query(self, task_query: typing.Any, worker_id: typing.Any):
        return (
            update(self.task_model)
            .where(self.task_model.id.in_(task_query))
            .values(
                state=models.TaskState.PROCESSING,
                worker_id=worker_id,
            )
            .returning(self.task_model.id)
        )

    async def dispatch(
        self,
        channels: typing.Sequence[str],
        worker_id: uuid.UUID,
        limit: int = 1,
        now: typing.Any = func.now(),
    ) -> list[models.Task]:
        task_query = self.make_task_query(channels, limit=limit, now=now)
        task_subquery = task_query.scalar_subquery()
        result = await self.session.execute(
            self.make_update_query(task_subquery, worker_id=worker_id)
        )
        task_ids = [item[0] for item in result]
        if not task_ids:
            return []
        tasks = await self.session.scalars(
            select(self.task_model).where(self.task_model.id.in_(task_ids))
        )
        return list(tasks.all())

    def _quote_channel(self, channel: str) -> str:
        bind = self.session.bind
        return bind.dialect.identifier_preparer.quote_identifier(channel)

    def _listen_conninfo(self) -> str:
        url = self.session.bind.url
        return url.set(drivername="postgresql").render_as_string(hide_password=False)

    async def listen(self, channels: typing.Sequence[str]):
        if self._listen_conn is None or self._listen_conn.closed:
            self._listen_conn = await psycopg.AsyncConnection.connect(
                self._listen_conninfo(),
                autocommit=True,
            )
        for channel in channels:
            quoted_channel = self._quote_channel(channel)
            await self._listen_conn.execute(f"LISTEN {quoted_channel}")

    async def poll(self, timeout: int = 5) -> list[Notification]:
        if self._listen_conn is None:
            raise RuntimeError("listen() must be called before poll()")

        notifications: list[Notification] = []
        async for notify in self._listen_conn.notifies(timeout=timeout, stop_after=1):
            notifications.append(
                Notification(
                    pid=notify.pid,
                    channel=notify.channel,
                    payload=notify.payload,
                )
            )
        if not notifications:
            raise TimeoutError("Timeout waiting for new notifications")

        async for notify in self._listen_conn.notifies(timeout=0):
            notifications.append(
                Notification(
                    pid=notify.pid,
                    channel=notify.channel,
                    payload=notify.payload,
                )
            )
        return notifications

    async def notify(self, channels: typing.Sequence[str]):
        conn = await self.session.connection()
        for channel in channels:
            quoted_channel = conn.dialect.identifier_preparer.quote_identifier(channel)
            await conn.exec_driver_sql(f"NOTIFY {quoted_channel}")

    async def aclose(self):
        if self._listen_conn is not None and not self._listen_conn.closed:
            await self._listen_conn.close()
        self._listen_conn = None
