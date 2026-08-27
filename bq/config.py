import json
import typing

from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator
from pydantic import PostgresDsn
from pydantic import ValidationInfo
from pydantic_core import MultiHostUrl
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict

_DEFAULT_ASYNC_SCHEME = "postgresql+psycopg"

_URL_SCHEME_REPLACEMENTS: tuple[tuple[str, str], ...] = (
    ("postgresql+psycopg2cffi://", f"{_DEFAULT_ASYNC_SCHEME}://"),
    ("postgresql+psycopg2://", f"{_DEFAULT_ASYNC_SCHEME}://"),
    ("postgresql+asyncpg://", f"{_DEFAULT_ASYNC_SCHEME}://"),
    ("postgresql://", f"{_DEFAULT_ASYNC_SCHEME}://"),
    ("postgres://", f"{_DEFAULT_ASYNC_SCHEME}://"),
)


def normalize_database_url(url: str) -> str:
    """Return a SQLAlchemy URL that uses the psycopg3 asyncio driver."""
    if url.startswith(f"{_DEFAULT_ASYNC_SCHEME}://"):
        return url
    for old, new in _URL_SCHEME_REPLACEMENTS:
        if url.startswith(old):
            return new + url[len(old) :]
    return url


class Config(BaseSettings):
    # Packages to scan for processor functions
    PROCESSOR_PACKAGES: list[str] = Field(default_factory=list)

    # Size of tasks batch to fetch each time from the database
    BATCH_SIZE: int = 1

    # Maximum number of tasks to process concurrently
    # Set to 1 to process tasks sequentially
    # Set to 0 to use the default (10)
    MAX_CONCURRENT_TASKS: int = 1
    # Deprecated alias for MAX_CONCURRENT_TASKS (BQ_MAX_WORKER_THREADS)
    MAX_WORKER_THREADS: int | None = None

    # How long we should poll before timeout in seconds
    POLL_TIMEOUT: int = 60

    # Interval of worker heartbeat update cycle in seconds
    WORKER_HEARTBEAT_PERIOD: int = 30

    # Timeout of worker heartbeat in seconds
    WORKER_HEARTBEAT_TIMEOUT: int = 100

    # which task model to use
    TASK_MODEL: str = "bq.Task"

    # which worker model to use
    WORKER_MODEL: str = "bq.Worker"

    # which event model to use
    EVENT_MODEL: str | None = "bq.Event"

    # Enable metrics HTTP server
    METRICS_HTTP_SERVER_ENABLED: bool = False

    # the metrics http server interface to listen
    METRICS_HTTP_SERVER_INTERFACE: str = ""

    # the metrics http server port to listen
    METRICS_HTTP_SERVER_PORT: int = 8000

    # default log level for metrics http server
    METRICS_HTTP_SERVER_LOG_LEVEL: int = 30

    # Optional logging.config dict for the metrics HTTP server (uvicorn).
    # When unset, a default config is used. Pass a dict programmatically or
    # JSON via BQ_METRICS_HTTP_SERVER_LOG_CONFIG.
    METRICS_HTTP_SERVER_LOG_CONFIG: dict[str, typing.Any] | None = None

    POSTGRES_SERVER: str = "localhost"
    POSTGRES_USER: str = "bq"
    POSTGRES_PASSWORD: str = ""
    POSTGRES_DB: str = "bq"
    # The URL of postgresql database to connect
    DATABASE_URL: typing.Optional[PostgresDsn] = None

    def resolved_max_concurrent_tasks(self) -> int:
        if self.MAX_CONCURRENT_TASKS == 0:
            return 10
        return self.MAX_CONCURRENT_TASKS

    @model_validator(mode="before")
    @classmethod
    def compat_max_worker_threads(cls, data: typing.Any) -> typing.Any:
        if not isinstance(data, dict):
            return data
        concurrent = data.get("MAX_CONCURRENT_TASKS")
        threads = data.get("MAX_WORKER_THREADS")
        if concurrent is None and threads is not None:
            data = dict(data)
            data["MAX_CONCURRENT_TASKS"] = threads
        elif concurrent is not None and threads is None:
            data = dict(data)
            data["MAX_WORKER_THREADS"] = concurrent
        return data

    @model_validator(mode="after")
    def fill_max_worker_threads_alias(self):
        if self.MAX_WORKER_THREADS is None:
            object.__setattr__(self, "MAX_WORKER_THREADS", self.MAX_CONCURRENT_TASKS)
        return self

    @field_validator("METRICS_HTTP_SERVER_LOG_CONFIG", mode="before")
    @classmethod
    def parse_metrics_log_config(cls, v: typing.Any) -> typing.Any:
        if v is None or isinstance(v, dict):
            return v
        if isinstance(v, str):
            return json.loads(v)
        raise ValueError("Unexpected METRICS_HTTP_SERVER_LOG_CONFIG type")

    @field_validator("DATABASE_URL", mode="before")
    @classmethod
    def assemble_db_connection(
        cls, v: typing.Optional[str], info: ValidationInfo
    ) -> typing.Any:
        if isinstance(v, str):
            return normalize_database_url(v)
        # Notice: Older Pydantic version (2.7), PostgresDsn is an annotated MultiHostUrl object,
        #         we cannot use isinstance with PostgresDsn directly. We need to check and see if PostgresDsn
        #         is an annotated type or not before we decide how to check if the passed in object is an
        #         PostgresDsn or not.
        if typing.get_origin(PostgresDsn) is typing.Annotated:
            if isinstance(v, MultiHostUrl):
                return normalize_database_url(str(v))
        else:
            if isinstance(v, PostgresDsn):
                return normalize_database_url(str(v))
        if v is not None:
            raise ValueError("Unexpected DATABASE_URL type")
        return PostgresDsn.build(
            scheme=_DEFAULT_ASYNC_SCHEME,
            username=info.data.get("POSTGRES_USER"),
            password=info.data.get("POSTGRES_PASSWORD"),
            host=info.data.get("POSTGRES_SERVER"),
            path=f"{info.data.get('POSTGRES_DB') or ''}",
        )

    model_config = SettingsConfigDict(
        case_sensitive=True,
        env_prefix="BQ_",
        populate_by_name=True,
    )
