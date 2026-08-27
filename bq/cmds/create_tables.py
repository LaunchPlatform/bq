import asyncio

from .. import models  # noqa
from ..db.base import Base
from .cli import cli
from .environment import Environment
from .environment import pass_env


@cli.command(name="create_tables", help="Create BeanQueue tables")
@pass_env
def create_tables(env: Environment):
    asyncio.run(_create_tables(env))


async def _create_tables(env: Environment):
    async with env.app.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    env.logger.info("Done, tables created")
