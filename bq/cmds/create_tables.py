import logging

import click

from .. import models  # noqa
from ..db.base import Base
from .utils import load_app

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "-a", "--app", type=str, help='BeanQueue app object to use, e.g. "my_pkgs.bq.app"'
)
def main(
    app: str | None = None,
):
    app = load_app(app)
    Base.metadata.create_all(bind=app.engine)
    logger.info("Done, tables created")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
