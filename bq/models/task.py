import enum

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Enum
from sqlalchemy import func
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID

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
    # arguments
    args = Column(JSONB, nullable=True)
    # keyword arguments
    kwargs = Column(JSONB, nullable=True)
    # name of the processing worker
    worker = Column(String, nullable=True)
    # created datetime of the task
    created_at = Column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __tablename__ = "bq_tasks"

    def __repr__(self) -> str:
        items = [
            ("id", self.id),
            ("state", self.state),
            ("channel", self.channel),
            ("worker", self.worker),
        ]
        return f"<{self.__class__.__name__} {make_repr_attrs(items)}>"
