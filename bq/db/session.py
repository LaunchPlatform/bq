from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker

SessionMaker = sessionmaker()
Session = scoped_session(SessionMaker)
