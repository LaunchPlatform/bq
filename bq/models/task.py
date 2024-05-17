import enum

from sqlalchemy import Column
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


class Task(Base):
    id = Column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    # foreign key id of assigned worker
    worker_id = Column(
        UUID(as_uuid=True),
        ForeignKey("bq_workers.id", name="fk_workers_id"),
        nullable=True,
    )
    # current state of the task
    state = Column(
        Enum(TaskState),
        nullable=False,
        default=TaskState.PENDING,
        server_default=TaskState.PENDING.value,
        index=True,
    )
    # channel for workers and job creator to listen/notify
    channel = Column(String, nullable=False, index=True)
    # module of the processor function
    module = Column(String, nullable=False)
    # func name of the processor func
    func_name = Column(String, nullable=False)
    # keyword arguments
    kwargs = Column(JSONB, nullable=True)
    # Result of the task
    result = Column(JSONB, nullable=True)
    # Error message
    error_message = Column(String, nullable=True)
    # created datetime of the task
    created_at = Column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    worker = relationship("Worker", back_populates="tasks", uselist=False)

    __tablename__ = "bq_tasks"

    def __repr__(self) -> str:
        items = [
            ("id", self.id),
            ("state", self.state),
            ("channel", self.channel),
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


@event.listens_for(Task, "after_insert")
def task_insert_notify(mapper: Mapper, connection: Connection, target: Task):
    from .. import models

    if target.state != models.TaskState.PENDING:
        return
    notify_if_needed(connection, target)


@event.listens_for(Task, "after_update")
def task_update_notify(mapper: Mapper, connection: Connection, target: Task):
    from .. import models

    history = inspect(target).attrs.state.history
    if not history.has_changes():
        return
    if target.state != models.TaskState.PENDING:
        return
    notify_if_needed(connection, target)
