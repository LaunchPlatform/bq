import logging

import click

from .utils import load_app


@click.command()
@click.argument("channels", nargs=-1)
@click.option(
    "-a", "--app", type=str, help='BeanQueue app object to use, e.g. "my_pkgs.bq.app"'
)
def main(
    channels: tuple[str, ...],
    app: str | None = None,
):
    app = load_app(app)
    app.process_tasks(channels)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
