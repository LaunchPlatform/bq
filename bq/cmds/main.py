from . import create_tables  # noqa
from . import process  # noqa
from . import submit  # noqa
from .cli import cli

__ALL__ = [cli]

if __name__ == "__main__":
    cli()
