import typing

import pytest
from pydantic import PostgresDsn
from pydantic import ValidationError

from bq.config import Config
from bq.config import normalize_database_url


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, PostgresDsn("postgresql+psycopg://bq@localhost/bq")),
        (
            "postgresql://myuser@example.com/mydb",
            PostgresDsn("postgresql+psycopg://myuser@example.com/mydb"),
        ),
        (
            "postgresql+psycopg://myuser@example.com/mydb",
            PostgresDsn("postgresql+psycopg://myuser@example.com/mydb"),
        ),
        (
            PostgresDsn("postgresql://myuser@example.com/mydb"),
            PostgresDsn("postgresql+psycopg://myuser@example.com/mydb"),
        ),
    ],
)
def test_database_url(value: typing.Any, expected: PostgresDsn):
    assert Config(DATABASE_URL=value).DATABASE_URL == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        ("postgresql://bq@localhost/bq", "postgresql+psycopg://bq@localhost/bq"),
        (
            "postgresql+psycopg2://bq@localhost/bq",
            "postgresql+psycopg://bq@localhost/bq",
        ),
        (
            "postgresql+psycopg://bq@localhost/bq",
            "postgresql+psycopg://bq@localhost/bq",
        ),
        ("postgres://bq@localhost/bq", "postgresql+psycopg://bq@localhost/bq"),
    ],
)
def test_normalize_database_url(value: str, expected: str):
    assert normalize_database_url(value) == expected


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
