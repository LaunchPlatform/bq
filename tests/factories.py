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
    channels = ["default"]
    last_heartbeat = func.now()
    created_at = func.now()

    class Meta:
        model = models.Worker
        sqlalchemy_session_persistence = "commit"


class TaskFactory(BaseFactory):
    state = models.TaskState.PENDING
    channel = Faker("slug")
    module = Faker("slug")
    func_name = Faker("slug")
    worker = None
    created_at = func.now()
    scheduled_at = None
    kwargs = {}

    class Meta:
        model = models.Task
        sqlalchemy_session_persistence = "commit"


class EventFactory(BaseFactory):
    type = models.EventType.COMPLETE
    task = SubFactory(TaskFactory)
    created_at = func.now()
    error_message = None
    scheduled_at = None

    class Meta:
        model = models.Event
        sqlalchemy_session_persistence = "commit"
