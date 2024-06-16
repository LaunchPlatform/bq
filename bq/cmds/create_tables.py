from .. import models  # noqa
from ..db.base import Base
from .cli import cli
from .environment import Environment
from .environment import pass_env


@cli.command(name="create_tables", help="Create BeanQueue tables")
@pass_env
def create_tables(env: Environment):
    Base.metadata.create_all(bind=env.app.engine)
    env.logger.info("Done, tables created")
