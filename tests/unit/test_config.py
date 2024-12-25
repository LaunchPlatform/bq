import typing

import pytest
from pydantic import PostgresDsn

from bq.config import Config


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, PostgresDsn("postgresql://bq@localhost/bq")),
        (
            "postgresql://myuser@example.com/mydb",
            PostgresDsn("postgresql://myuser@example.com/mydb"),
        ),
        (
            PostgresDsn("postgresql://myuser@example.com/mydb"),
            PostgresDsn("postgresql://myuser@example.com/mydb"),
        ),
    ],
)
def test_database_url(value: typing.Any, expected: PostgresDsn):
    assert Config(DATABASE_URL=value).DATABASE_URL == expected
