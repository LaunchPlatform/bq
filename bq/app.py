import functools
import importlib
import json
import logging
import platform
import sys
import threading
import time
import typing
from wsgiref.simple_server import make_server
from wsgiref.simple_server import WSGIRequestHandler

import venusian
from sqlalchemy import func
from sqlalchemy.engine import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session as DBSession
from sqlalchemy.pool import SingletonThreadPool

from . import constants
from . import events
from . import models
from .config import Config
from .db.session import SessionMaker
from .processors.processor import Processor
from .processors.processor import ProcessorHelper
from .processors.registry import collect
from .services.dispatch import DispatchService
from .services.worker import WorkerService
from .utils import load_module_var

logger = logging.getLogger(__name__)


class WSGIRequestHandlerWithLogger(WSGIRequestHandler):
    logger = logging.getLogger("metrics_server")

    def log_message(self, format, *args):
        message = format % args
        self.logger.info(
            "%s - - [%s] %s\n"
            % (
                self.address_string(),
                self.log_date_time_string(),
                message.translate(self._control_char_table),
            )
        )


class BeanQueue:
    def __init__(
        self,
        config: Config | None = None,
        session_cls: DBSession = SessionMaker,
        worker_service_cls: typing.Type[WorkerService] = WorkerService,
        dispatch_service_cls: typing.Type[DispatchService] = DispatchService,
        engine: Engine | None = None,
    ):
        self.config = config if config is not None else Config()
        self.session_cls = session_cls
        self.worker_service_cls = worker_service_cls
        self.dispatch_service_cls = dispatch_service_cls
        self._engine = engine

    def create_default_engine(self):
        return create_engine(
            str(self.config.DATABASE_URL), poolclass=SingletonThreadPool
        )

    def make_session(self) -> DBSession:
        return self.session_cls(bind=self.engine)

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = self.create_default_engine()
        return self._engine

    @property
    def task_model(self) -> typing.Type[models.Task]:
        return load_module_var(self.config.TASK_MODEL)

    @property
    def worker_model(self) -> typing.Type[models.Worker]:
        return load_module_var(self.config.WORKER_MODEL)

    def _make_worker_service(self, session: DBSession):
        return self.worker_service_cls(
            session=session, task_model=self.task_model, worker_model=self.worker_model
        )

    def _make_dispatch_service(self, session: DBSession):
        return self.dispatch_service_cls(session=session, task_model=self.task_model)

    def processor(
        self,
        channel: str = constants.DEFAULT_CHANNEL,
        auto_complete: bool = True,
        auto_rollback_on_exc: bool = True,
        task_model: typing.Type | None = None,
    ) -> typing.Callable:
        def decorator(wrapped: typing.Callable):
            processor = Processor(
                module=wrapped.__module__,
                name=wrapped.__name__,
                channel=channel,
                func=wrapped,
                auto_complete=auto_complete,
                auto_rollback_on_exc=auto_rollback_on_exc,
            )
            helper_obj = ProcessorHelper(
                processor,
                task_cls=task_model if task_model is not None else self.task_model,
            )

            def callback(scanner: venusian.Scanner, name: str, ob: typing.Callable):
                if processor.name != name:
                    raise ValueError("Name is not the same")
                scanner.registry.add(processor)

            venusian.attach(
                helper_obj, callback, category=constants.BQ_PROCESSOR_CATEGORY
            )
            return helper_obj

        return decorator

    def update_workers(
        self,
        worker_id: typing.Any,
    ):
        db = self.make_session()

        worker_service = self._make_worker_service(db)
        dispatch_service = self._make_dispatch_service(db)

        current_worker = worker_service.get_worker(worker_id)
        logger.info(
            "Updating worker %s with heartbeat_period=%s, heartbeat_timeout=%s",
            current_worker.id,
            self.config.WORKER_HEARTBEAT_PERIOD,
            self.config.WORKER_HEARTBEAT_TIMEOUT,
        )
        while True:
            dead_workers = worker_service.fetch_dead_workers(
                timeout=self.config.WORKER_HEARTBEAT_TIMEOUT
            )
            task_count = worker_service.reschedule_dead_tasks(
                # TODO: a better way to abstract this?
                dead_workers.with_entities(current_worker.__class__.id)
            )
            found_dead_worker = False
            for dead_worker in dead_workers:
                found_dead_worker = True
                logger.info(
                    "Found dead worker %s (name=%s), reschedule %s dead tasks in channels %s",
                    dead_worker.id,
                    dead_worker.name,
                    task_count,
                    dead_worker.channels,
                )
                dispatch_service.notify(dead_worker.channels)
            if found_dead_worker:
                db.commit()

            if current_worker.state != models.WorkerState.RUNNING:
                # This probably means we are somehow very slow to update the heartbeat in time, or the timeout window
                # is set too short. It could also be the administrator update the worker state to something else than
                # RUNNING. Regardless the reason, let's stop processing.
                logger.warning(
                    "Current worker %s state is %s instead of running, quit processing",
                    current_worker.id,
                    current_worker.state,
                )
                sys.exit(0)

            time.sleep(self.config.WORKER_HEARTBEAT_PERIOD)
            current_worker.last_heartbeat = func.now()
            db.add(current_worker)
            db.commit()

    def _serve_http_request(
        self, worker_id: typing.Any, environ: dict, start_response: typing.Callable
    ) -> list[bytes]:
        path = environ["PATH_INFO"]
        if path == "/healthz":
            db = self.make_session()
            worker_service = self._make_worker_service(db)
            worker = worker_service.get_worker(worker_id)
            if worker is not None and worker.state == models.WorkerState.RUNNING:
                start_response(
                    "200 OK",
                    [
                        ("Content-Type", "application/json"),
                    ],
                )
                return [
                    json.dumps(dict(status="ok", worker_id=str(worker_id))).encode(
                        "utf8"
                    )
                ]
            else:
                logger.warning("Bad worker %s state %s", worker_id, worker.state)
                start_response(
                    "500 Internal Server Error",
                    [
                        ("Content-Type", "application/json"),
                    ],
                )
                return [
                    json.dumps(
                        dict(
                            status="internal error",
                            worker_id=str(worker_id),
                            state=str(worker.state),
                        )
                    ).encode("utf8")
                ]
        # TODO: add other metrics endpoints
        start_response(
            "404 NOT FOUND",
            [
                ("Content-Type", "application/json"),
            ],
        )
        return [json.dumps(dict(status="not found")).encode("utf8")]

    def run_metrics_http_server(self, worker_id: typing.Any):
        host = self.config.METRICS_HTTP_SERVER_INTERFACE
        port = self.config.METRICS_HTTP_SERVER_PORT
        with make_server(
            host,
            port,
            functools.partial(self._serve_http_request, worker_id),
            handler_class=WSGIRequestHandlerWithLogger,
        ) as httpd:
            logger.info("Run metrics HTTP server on %s:%s", host, port)
            httpd.serve_forever()

    def process_tasks(
        self,
        channels: tuple[str, ...],
    ):
        db = self.make_session()
        if not channels:
            channels = [constants.DEFAULT_CHANNEL]

        if not self.config.PROCESSOR_PACKAGES:
            logger.error("No PROCESSOR_PACKAGES provided")
            raise ValueError("No PROCESSOR_PACKAGES provided")

        logger.info("Scanning packages %s", self.config.PROCESSOR_PACKAGES)
        pkgs = list(map(importlib.import_module, self.config.PROCESSOR_PACKAGES))
        registry = collect(pkgs)
        for channel, module_processors in registry.processors.items():
            logger.info("Collected processors with channel %r", channel)
            for module, func_processors in module_processors.items():
                for processor in func_processors.values():
                    logger.info(
                        "  Processor module=%r, name=%r", module, processor.name
                    )

        dispatch_service = self.dispatch_service_cls(
            session=db, task_model=self.task_model
        )
        work_service = self.worker_service_cls(
            session=db, task_model=self.task_model, worker_model=self.worker_model
        )

        worker = work_service.make_worker(name=platform.node(), channels=channels)
        db.add(worker)
        dispatch_service.listen(channels)
        db.commit()

        metrics_server_thread = None
        if self.config.METRICS_HTTP_SERVER_ENABLED:
            WSGIRequestHandlerWithLogger.logger.setLevel(
                self.config.METRICS_HTTP_SERVER_LOG_LEVEL
            )
            metrics_server_thread = threading.Thread(
                target=self.run_metrics_http_server,
                args=(worker.id,),
            )
            metrics_server_thread.daemon = True
            metrics_server_thread.start()

        logger.info("Created worker %s, name=%s", worker.id, worker.name)
        events.worker_init.send(self, worker=worker)

        logger.info("Processing tasks in channels = %s ...", channels)

        worker_update_thread = threading.Thread(
            target=functools.partial(
                self.update_workers,
                worker_id=worker.id,
            ),
            name="update_workers",
        )
        worker_update_thread.daemon = True
        worker_update_thread.start()

        worker_id = worker.id

        try:
            while True:
                while True:
                    tasks = dispatch_service.dispatch(
                        channels,
                        worker_id=worker_id,
                        limit=self.config.BATCH_SIZE,
                    ).all()
                    for task in tasks:
                        logger.info(
                            "Processing task %s, channel=%s, module=%s, func=%s",
                            task.id,
                            task.channel,
                            task.module,
                            task.func_name,
                        )
                        # TODO: support processor pool and other approaches to dispatch the workload
                        registry.process(task)
                    if not tasks:
                        # we should try to keep dispatching until we cannot find tasks
                        break
                    else:
                        db.commit()
                # we will not see notifications in a transaction, need to close the transaction first before entering
                # polling
                db.close()
                try:
                    for notification in dispatch_service.poll(
                        timeout=self.config.POLL_TIMEOUT
                    ):
                        logger.debug("Receive notification %s", notification)
                except TimeoutError:
                    logger.debug("Poll timeout, try again")
                    continue
        except (SystemExit, KeyboardInterrupt):
            db.rollback()
            logger.info("Shutting down ...")
            worker_update_thread.join(5)
            if metrics_server_thread is not None:
                metrics_server_thread.join(5)

        worker.state = models.WorkerState.SHUTDOWN
        db.add(worker)
        task_count = self.worker_service_cls.reschedule_dead_tasks([worker.id])
        logger.info("Reschedule %s tasks", task_count)
        dispatch_service.notify(channels)
        db.commit()

        logger.info("Shutdown gracefully")
