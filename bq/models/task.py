from sqlalchemy import Column
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import UUID

from ..db.base import Base
from .helpers import make_repr_attrs


class Task(Base):
    id = Column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )

    __tablename__ = "bq_tasks"

    def __repr__(self) -> str:
        items = [
            ("id", self.id),
            # TODO: add more stuff
        ]
        return f"<{self.__class__.__name__} {make_repr_attrs(items)}>"
