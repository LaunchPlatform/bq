import dataclasses
import select
import typing
import uuid

from sqlalchemy.orm import Query

from .. import models
from ..db.session import Session


@dataclasses.dataclass(frozen=True)
class Notification:
    pid: int
    channel: str
    payload: typing.Optional[str] = None


class DispatchService:
    def __init__(self, session: Session):
        self.session = session

    def make_task_query(self, channels: typing.Sequence[str], limit: int = 1) -> Query:
        return (
            self.session.query(models.Task.id)
            .filter(models.Task.channel.in_(channels))
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
        self, channels: typing.Sequence[str], worker: models.Worker, limit: int = 1
    ) -> Query:
        task_query = self.make_task_query(channels, limit=limit)
        task_subquery = task_query.scalar_subquery()
        task_ids = [
            item[0]
            for item in self.session.execute(
                self.make_update_query(task_subquery, worker_id=worker.id)
            )
        ]
        # TODO: ideally returning with (models.Task) should return the whole model, but SQLAlchemy is returning
        #       it columns in rows. We can save a round trip if we can find out how to solve this
        return self.session.query(models.Task).filter(models.Task.id.in_(task_ids))

    def listen(self, channels: typing.Sequence[str]):
        conn = self.session.connection()
        for channel in channels:
            quoted_channel = conn.dialect.identifier_preparer.quote_identifier(channel)
            conn.exec_driver_sql(f"LISTEN {quoted_channel}")

    def poll(self, timeout: int = 5) -> typing.Generator[Notification, None, None]:
        conn = self.session.connection()
        driver_conn = conn.connection.driver_connection

        def pop_notifies():
            while driver_conn.notifies:
                notify = driver_conn.notifies.pop(0)
                yield Notification(
                    pid=notify.pid,
                    channel=notify.channel,
                    payload=notify.payload,
                )

        # poll first to see if there's anything already
        driver_conn.poll()
        if driver_conn.notifies:
            yield from pop_notifies()
        else:
            # okay, nothing, let's select and wait for new stuff
            if select.select([driver_conn], [], [], timeout) == ([], [], []):
                # nope, nothing, times out
                raise TimeoutError("Timeout waiting for new notifications")
            else:
                # yep, we got something
                driver_conn.poll()
                yield from pop_notifies()

    def notify(self, channels: typing.Sequence[str]):
        conn = self.session.connection()
        for channel in channels:
            quoted_channel = conn.dialect.identifier_preparer.quote_identifier(channel)
            conn.exec_driver_sql(f"NOTIFY {quoted_channel}")
