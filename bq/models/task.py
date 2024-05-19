import datetime
import enum
import typing
import uuid

from sqlalchemy import Connection
from sqlalchemy import DateTime
from sqlalchemy import Enum
from sqlalchemy import event
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Mapper
from sqlalchemy.orm import relationship

from ..db.base import Base
from .helpers import make_repr_attrs


class TaskState(enum.Enum):
    # task just created, not scheduled yet
    PENDING = "PENDING"
    # a worker is processing the task right now
    PROCESSING = "PROCESSING"
    # the task is done
    DONE = "DONE"
    # the task is failed
    FAILED = "FAILED"


class TaskModelMixin:
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    # current state of the task
    state: Mapped[TaskState] = mapped_column(
        Enum(TaskState),
        nullable=False,
        default=TaskState.PENDING,
        server_default=TaskState.PENDING.value,
        index=True,
    )
    # channel for workers and job creator to listen/notify
    channel: Mapped[str] = mapped_column(String, nullable=False, index=True)
    # module of the processor function
    module: Mapped[str] = mapped_column(String, nullable=False)
    # func name of the processor func
    func_name: Mapped[str] = mapped_column(String, nullable=False)
    # keyword arguments
    kwargs: Mapped[typing.Optional[typing.Any]] = mapped_column(JSONB, nullable=True)
    # Result of the task
    result: Mapped[typing.Optional[typing.Any]] = mapped_column(JSONB, nullable=True)
    # Error message
    error_message: Mapped[typing.Optional[str]] = mapped_column(String, nullable=True)
    # created datetime of the task
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class TaskModelRefWorkerMixin:
    # foreign key id of assigned worker
    worker_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("bq_workers.id", name="fk_workers_id"),
        nullable=True,
    )

    @declared_attr
    def worker(cls) -> Mapped["Worker"]:
        return relationship("Worker", back_populates="tasks", uselist=False)


class Task(TaskModelMixin, TaskModelRefWorkerMixin, Base):
    __tablename__ = "bq_tasks"

    def __repr__(self) -> str:
        items = [
            ("id", self.id),
            ("state", self.state),
            ("channel", self.channel),
            ("module", self.module),
            ("func_name", self.func_name),
        ]
        return f"<{self.__class__.__name__} {make_repr_attrs(items)}>"


def notify_if_needed(connection: Connection, task: Task):
    session = inspect(task).session
    transaction = session.get_transaction()
    if transaction is not None:
        key = "_notified_channels"
        if hasattr(transaction, key):
            notified_channels = getattr(transaction, key)
        else:
            notified_channels = set()
            setattr(transaction, key, notified_channels)

        if task.channel in notified_channels:
            # already notified, skip
            return
        notified_channels.add(task.channel)

    quoted_channel = connection.dialect.identifier_preparer.quote_identifier(
        task.channel
    )
    connection.exec_driver_sql(f"NOTIFY {quoted_channel}")


def task_insert_notify(mapper: Mapper, connection: Connection, target: Task):
    if target.state != TaskState.PENDING:
        return
    notify_if_needed(connection, target)


def task_update_notify(mapper: Mapper, connection: Connection, target: Task):
    history = inspect(target).attrs.state.history
    if not history.has_changes():
        return
    if target.state != TaskState.PENDING:
        return
    notify_if_needed(connection, target)


def listen_events(model_cls: typing.Type):
    event.listens_for(model_cls, "after_insert")(task_insert_notify)
    event.listens_for(model_cls, "after_update")(task_update_notify)


listen_events(Task)
