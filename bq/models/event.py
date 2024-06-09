import datetime
import enum
import typing
import uuid

from sqlalchemy import DateTime
from sqlalchemy import Enum
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship

from ..db.base import Base
from .helpers import make_repr_attrs


class EventType(enum.Enum):
    # task failed
    FAILED = "FAILED"
    # task failed and retry scheduled
    FAILED_RETRY_SCHEDULED = "FAILED_RETRY_SCHEDULED"
    # task complete
    COMPLETE = "COMPLETE"


class EventModelMixin:
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    # type of the event
    type: Mapped[EventType] = mapped_column(
        Enum(EventType),
        nullable=False,
        index=True,
    )
    # Error message
    error_message: Mapped[typing.Optional[str]] = mapped_column(String, nullable=True)
    # the scheduled at time for retry
    scheduled_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    # created datetime of the event
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class EventModelRefTaskMixin:
    # foreign key id of the task
    task_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("bq_tasks.id", name="fk_event_task_id"),
        nullable=True,
    )

    @declared_attr
    def task(cls) -> Mapped["Task"]:
        return relationship("Task", back_populates="events", uselist=False)


class Event(EventModelMixin, EventModelRefTaskMixin, Base):
    __tablename__ = "bq_events"

    def __repr__(self) -> str:
        items = [
            ("id", self.id),
            ("type", self.type),
            ("created_at", self.created_at),
            ("scheduled_at", self.scheduled_at),
        ]
        return f"<{self.__class__.__name__} {make_repr_attrs(items)}>"
