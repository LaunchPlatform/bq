import enum

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Enum
from sqlalchemy import func
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from ..db.base import Base
from .helpers import make_repr_attrs


class WorkerState(enum.Enum):
    # the worker is running
    RUNNING = "RUNNING"
    # the worker shuts down normally
    SHUTDOWN = "SHUTDOWN"
    # The worker has no heartbeat for a while
    NO_HEARTBEAT = "NO_HEARTBEAT"


class Worker(Base):
    id = Column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    # current state of the worker
    state = Column(
        Enum(WorkerState),
        nullable=False,
        default=WorkerState.RUNNING,
        server_default=WorkerState.RUNNING.value,
        index=True,
    )
    # name of the worker
    name = Column(String, nullable=False)
    # the channels we are processing
    channels = Column(ARRAY(String), nullable=False)
    # last heartbeat of this worker
    last_heartbeat = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        index=True,
    )
    # created datetime of the worker
    created_at = Column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    tasks = relationship(
        "Task",
        back_populates="worker",
        cascade="all,delete",
        order_by="Task.created_at",
    )

    __tablename__ = "bq_workers"

    def __repr__(self) -> str:
        items = [
            ("id", self.id),
            ("name", self.name),
            ("channels", self.channels),
            ("state", self.state),
        ]
        return f"<{self.__class__.__name__} {make_repr_attrs(items)}>"
