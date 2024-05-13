from factory import Faker
from factory import SubFactory
from factory.alchemy import SQLAlchemyModelFactory
from sqlalchemy import func

from bq import models
from bq.db.session import Session


class BaseFactory(SQLAlchemyModelFactory):
    class Meta:
        abstract = True
        sqlalchemy_session = Session


class WorkerFactory(BaseFactory):
    state = models.WorkerState.RUNNING
    name = Faker("slug")
    last_heartbeat = func.now()
    created_at = func.now()

    class Meta:
        model = models.Worker
        sqlalchemy_session_persistence = "commit"


class TaskFactory(BaseFactory):
    state = models.TaskState.PENDING
    channel = Faker("slug")
    worker = None
    created_at = func.now()
    args = []
    kwargs = {}

    class Meta:
        model = models.Task
        sqlalchemy_session_persistence = "commit"
