import typing

import pytest
from pydantic import PostgresDsn
from pydantic import ValidationError

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


@pytest.mark.parametrize(
    "value",
    [
        1234,
        12.34,
        object(),
        list(),
        dict(),
    ],
)
def test_bad_database_url_type(value: typing.Any):
    with pytest.raises(ValidationError):
        Config(DATABASE_URL=value)
