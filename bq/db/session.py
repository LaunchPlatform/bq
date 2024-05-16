from sqlalchemy import event
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import Session as DBSession
from sqlalchemy.orm import sessionmaker

SessionMaker = sessionmaker()
Session = scoped_session(SessionMaker)


def connect_events(session: DBSession):
    from ..models.task import task_insert_notify
    from ..models.task import task_update_notify

    event.listen(session, "after_insert", task_insert_notify)
    event.listen(session, "after_update", task_update_notify)
