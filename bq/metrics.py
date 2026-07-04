from __future__ import annotations

import copy
import logging.config
import threading
import typing
from collections.abc import Callable
from collections.abc import Coroutine
from importlib.util import find_spec

from sqlalchemy.orm import Session as DBSession

from . import events
from . import models

if typing.TYPE_CHECKING:
    from .app import BeanQueue
    from .config import Config

logger = logging.getLogger(__name__)

METRICS_EXTRA = "metrics"
METRICS_SERVER_LOGGER = "metrics_server"


def _healthz_sync_wrapper(
    func: Callable[..., typing.Any],
) -> Callable[..., Coroutine[typing.Any, typing.Any, typing.Any]]:
    async def wrapper(sender: typing.Any, **kwargs: typing.Any) -> typing.Any:
        return func(sender, **kwargs)

    return wrapper


class MetricsExtrasNotInstalledError(ImportError):
    """Raised when metrics optional dependencies are not installed."""


def require_metrics_extras() -> None:
    missing = []
    if find_spec("starlette") is None:
        missing.append("starlette")
    if find_spec("uvicorn") is None:
        missing.append("uvicorn")
    if missing:
        raise MetricsExtrasNotInstalledError(
            "Health check and metrics HTTP server require optional dependencies "
            f"({', '.join(missing)}). "
            f"Install them with: pip install beanqueue[{METRICS_EXTRA}]"
        )


def default_metrics_log_config(log_level: int) -> dict[str, typing.Any]:
    from uvicorn.config import LOGGING_CONFIG

    level_name = logging.getLevelName(log_level)
    info_level = logging.getLevelName(logging.INFO)
    log_config = copy.deepcopy(LOGGING_CONFIG)
    log_config["handlers"]["access"]["stream"] = "ext://sys.stderr"
    log_config["loggers"]["uvicorn"]["level"] = level_name
    log_config["loggers"]["uvicorn.error"]["level"] = level_name
    # Access lines are logged at INFO by uvicorn regardless of server log level.
    log_config["loggers"]["uvicorn.access"]["level"] = info_level
    log_config["loggers"][METRICS_SERVER_LOGGER] = {
        "handlers": ["default"],
        "level": info_level,
        "propagate": False,
    }
    return log_config


def resolve_metrics_log_config(config: Config) -> dict[str, typing.Any]:
    if config.METRICS_HTTP_SERVER_LOG_CONFIG is not None:
        return config.METRICS_HTTP_SERVER_LOG_CONFIG
    return default_metrics_log_config(config.METRICS_HTTP_SERVER_LOG_LEVEL)


class MetricsServer:
    def __init__(self, bq: BeanQueue, worker_id: typing.Any):
        require_metrics_extras()
        self._bq = bq
        self._worker_id = worker_id
        self._server = None
        self._thread: threading.Thread | None = None

    def _has_custom_health_checks(self) -> bool:
        return bool(events.healthz_check.receivers)

    async def _run_healthz_checks(
        self, worker: models.Worker, session: DBSession, body: dict[str, typing.Any]
    ) -> bool:
        try:
            await events.healthz_check.send_async(
                self._bq,
                _sync_wrapper=_healthz_sync_wrapper,
                worker=worker,
                session=session,
            )
        except Exception as exc:
            logger.exception("Custom healthz check failed")
            body["error"] = str(exc)
            return False
        return True

    async def check_healthz(self) -> tuple[bool, dict[str, typing.Any]]:
        body: dict[str, typing.Any] = {"status": "ok"}

        if not self._has_custom_health_checks():
            return True, body

        with self._bq.make_session() as db:
            worker_service = self._bq._make_worker_service(db)
            worker = worker_service.get_worker(self._worker_id)
            body["worker_id"] = str(self._worker_id)

            if not await self._run_healthz_checks(worker, db, body):
                body["status"] = "internal error"
                return False, body
        return True, body

    def create_app(self):
        from starlette.applications import Starlette
        from starlette.responses import JSONResponse
        from starlette.routing import Route

        async def healthz(_request):
            ok, body = await self.check_healthz()
            return JSONResponse(body, status_code=200 if ok else 500)

        return Starlette(
            routes=[
                Route("/healthz", healthz),
            ]
        )

    def start(self) -> None:
        import uvicorn

        require_metrics_extras()
        host = self._bq.config.METRICS_HTTP_SERVER_INTERFACE
        port = self._bq.config.METRICS_HTTP_SERVER_PORT
        log_config = resolve_metrics_log_config(self._bq.config)
        logging.config.dictConfig(log_config)

        app = self.create_app()
        # log_level is intentionally omitted: uvicorn would override logger levels
        # from log_config (including uvicorn.access) after configure_logging().
        config = uvicorn.Config(
            app,
            host=host,
            port=port,
            log_config=log_config,
            access_log=True,
        )
        self._server = uvicorn.Server(config)

        def run() -> None:
            logging.getLogger(METRICS_SERVER_LOGGER).info(
                "Run metrics HTTP server on %s:%s", host, port
            )
            self._server.run()

        self._thread = threading.Thread(target=run, name="metrics_server")
        self._thread.daemon = True
        self._thread.start()

    def shutdown(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self._thread is not None:
            self._thread.join(1)
