import functools
import importlib
import json
import logging
import platform
import socketserver
import sys
import threading
import typing
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait as futures_wait
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version
from wsgiref.simple_server import make_server
from wsgiref.simple_server import WSGIRequestHandler
from wsgiref.simple_server import WSGIServer

import venusian
from sqlalchemy import func
from sqlalchemy.engine import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session as DBSession
from sqlalchemy.pool import NullPool
from sqlalchemy.pool import QueuePool
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

class ThreadingWSGIServer(socketserver.ThreadingMixIn, WSGIServer):
    daemon_threads = True

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
        self._worker_update_shutdown_event: threading.Event = threading.Event()
        # noop if metrics thread is not started yet, shutdown if it is started
        self._metrics_server_shutdown: typing.Callable[[], None] = lambda: None
        # Health state as atomic tuple: (is_ok, info_dict)
        # Written by heartbeat/main threads, read by HTTP handler threads
        self._health_state: tuple[bool, dict] = (False, {})

    def create_default_engine(self):
        # Use thread-safe connection pool when thread pool executor is enabled
        if self.config.MAX_WORKER_THREADS != 1:
            # QueuePool is thread-safe and suitable for multi-threaded usage
            # Configure pool size based on number of worker threads
            max_workers = self.config.MAX_WORKER_THREADS if self.config.MAX_WORKER_THREADS > 0 else 10
            pool_size = max_workers + 5  # Extra connections for main thread and worker update thread
            return create_engine(
                str(self.config.DATABASE_URL),
                poolclass=QueuePool,
                pool_size=pool_size,
                max_overflow=10,
            )
        else:
            # SingletonThreadPool for single-threaded sequential processing
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

    @property
    def event_model(self) -> typing.Type[models.Event] | None:
        if self.config.EVENT_MODEL is None:
            return
        return load_module_var(self.config.EVENT_MODEL)

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
        retry_policy: typing.Callable | None = None,
        retry_exceptions: typing.Type | typing.Tuple[typing.Type, ...] | None = None,
        task_model: typing.Type | None = None,
    ) -> typing.Callable:
        def decorator(wrapped: typing.Callable):
            processor = Processor(
                module=wrapped.__module__,
                name=wrapped.__name__,
                channel=channel,
                func=wrapped,
                auto_complete=auto_complete,
                retry_policy=retry_policy,
                retry_exceptions=retry_exceptions,
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
                self._health_state = (False, {"state": str(current_worker.state)})
                # This probably means we are somehow very slow to update the heartbeat in time, or the timeout window
                # is set too short. It could also be the administrator update the worker state to something else than
                # RUNNING. Regardless the reason, let's stop processing.
                logger.warning(
                    "Current worker %s state is %s instead of running, quit processing",
                    current_worker.id,
                    current_worker.state,
                )
                sys.exit(0)

            do_shutdown = self._worker_update_shutdown_event.wait(
                self.config.WORKER_HEARTBEAT_PERIOD
            )
            if do_shutdown:
                return

            current_worker.last_heartbeat = func.now()
            db.add(current_worker)
            db.commit()
            self._health_state = (
                current_worker.state == models.WorkerState.RUNNING,
                {"state": str(current_worker.state)},
            )

    def _serve_http_request(
        self, worker_id: typing.Any, environ: dict, start_response: typing.Callable
    ) -> list[bytes]:
        path = environ["PATH_INFO"]
        if path == "/healthz":
            health_ok, health_info = self._health_state
            if health_ok:
                start_response("200 OK", [("Content-Type", "application/json")])
                return [json.dumps(dict(
                    status="ok",
                    worker_id=str(worker_id),
                    **health_info,
                )).encode("utf8")]
            else:
                start_response(
                    "500 Internal Server Error",
                    [("Content-Type", "application/json")],
                )
                return [json.dumps(dict(
                    status="error",
                    worker_id=str(worker_id),
                    **health_info,
                )).encode("utf8")]
        start_response("404 NOT FOUND", [("Content-Type", "application/json")])
        return [json.dumps(dict(status="not found")).encode("utf8")]

    def run_metrics_http_server(self, worker_id: typing.Any):
        host = self.config.METRICS_HTTP_SERVER_INTERFACE
        port = self.config.METRICS_HTTP_SERVER_PORT
        with make_server(
            host,
            port,
            functools.partial(self._serve_http_request, worker_id),
            handler_class=WSGIRequestHandlerWithLogger,
            server_class=ThreadingWSGIServer,
        ) as httpd:
            # expose graceful shutdown to the main thread
            self._metrics_server_shutdown = httpd.shutdown
            logger.info("Run metrics HTTP server on %s:%s", host, port)
            httpd.serve_forever()

    def _process_task_in_thread(
        self,
        task_id: typing.Any,
        registry: typing.Any,
    ):
        """Process a single task in a thread-safe manner with its own database session.

        This method is called from worker threads in the thread pool. It creates its own
        database session to avoid SQLAlchemy session conflicts between threads.
        """
        db = self.make_session()
        try:
            # Reload the task in this thread's session to avoid SQLAlchemy context issues
            task = db.query(self.task_model).filter(self.task_model.id == task_id).one()

            logger.info(
                "Processing task %s, channel=%s, module=%s, func=%s",
                task.id,
                task.channel,
                task.module,
                task.func_name,
            )
            registry.process(task, event_cls=self.event_model)
            db.commit()
        except Exception as e:
            logger.exception("Error processing task %s: %s", task_id, e)
            db.rollback()
            raise
        finally:
            db.close()

    def _process_tasks_sequential(
        self,
        db: DBSession,
        dispatch_service: DispatchService,
        registry: typing.Any,
        channels: tuple[str, ...],
        worker_id: typing.Any,
    ):
        """Process tasks sequentially (original behavior for MAX_WORKER_THREADS=1)."""
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
                    registry.process(task, event_cls=self.event_model)
                if tasks:
                    db.commit()

                if not tasks:
                    break

            db.close()
            try:
                for notification in dispatch_service.poll(
                    timeout=self.config.POLL_TIMEOUT
                ):
                    logger.debug("Receive notification %s", notification)
            except TimeoutError:
                logger.debug("Poll timeout, try again")
                continue

    def _process_tasks_threaded(
        self,
        db: DBSession,
        executor: ThreadPoolExecutor,
        dispatch_service: DispatchService,
        registry: typing.Any,
        channels: tuple[str, ...],
        worker_id: typing.Any,
    ):
        """Process tasks using thread pool with continuous task feeding.

        This implementation continuously checks for completed futures and fetches new tasks
        when there's capacity in the thread pool. It uses concurrent.futures.wait() to
        properly detect ANY completed future, not just the first one submitted.
        """
        max_workers = self.config.MAX_WORKER_THREADS
        if max_workers == 0:
            max_workers = 10  # Default when set to auto

        running_futures: set = set()

        while True:
            # Clean up ANY completed futures using wait() with zero timeout
            if running_futures:
                done, running_futures = futures_wait(
                    running_futures, timeout=0, return_when=FIRST_COMPLETED
                )
                for f in done:
                    try:
                        f.result()
                    except Exception as e:
                        logger.error("Task processing failed: %s", e)

            # If we have capacity, fetch and submit more tasks
            capacity = max_workers - len(running_futures)
            if capacity > 0:
                tasks = dispatch_service.dispatch(
                    channels,
                    worker_id=worker_id,
                    limit=min(capacity, self.config.BATCH_SIZE),
                ).all()

                # Always commit to close the transaction and refresh the snapshot,
                # so subsequent dispatch calls can see newly committed tasks
                db.commit()

                if tasks:
                    logger.debug(
                        "Dispatching %d tasks (running=%d, capacity=%d)",
                        len(tasks), len(running_futures), capacity
                    )

                    for task in tasks:
                        future = executor.submit(
                            self._process_task_in_thread,
                            task.id,
                            registry,
                        )
                        running_futures.add(future)

            # If we have running tasks, wait briefly for any to complete then check for new tasks
            if running_futures:
                # Short wait - allows checking for new tasks frequently
                done, running_futures = futures_wait(
                    running_futures, timeout=0.05, return_when=FIRST_COMPLETED
                )
                for f in done:
                    try:
                        f.result()
                    except Exception as e:
                        logger.error("Task processing failed: %s", e)
                continue

            # No running tasks and no new tasks found - poll for notifications
            db.close()
            try:
                for notification in dispatch_service.poll(
                    timeout=self.config.POLL_TIMEOUT
                ):
                    logger.debug("Receive notification %s", notification)
            except TimeoutError:
                logger.debug("Poll timeout, try again")
                continue

    def process_tasks(
        self,
        channels: tuple[str, ...],
    ):
        try:
            bq_version = version("beanqueue")
        except PackageNotFoundError:
            bq_version = "unknown"

        logger.info(
            "Starting processing tasks, bq_version=%s",
            bq_version,
        )
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
        self._health_state = (True, {"state": "RUNNING"})

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
        # Graceful shutdown of worker update event on exit of the worker
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

        # Determine the number of worker threads
        max_workers = self.config.MAX_WORKER_THREADS
        if max_workers == 0:
            max_workers = None  # Default to (num_cpus * 5)

        # Create thread pool executor for concurrent task processing
        executor = None
        if max_workers != 1:
            executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="task_worker")
            logger.info("Created thread pool executor with max_workers=%s", max_workers)

        try:
            if executor is not None:
                # Threaded processing with continuous task feeding
                self._process_tasks_threaded(
                    db=db,
                    executor=executor,
                    dispatch_service=dispatch_service,
                    registry=registry,
                    channels=channels,
                    worker_id=worker_id,
                )
            else:
                # Sequential processing (original behavior)
                self._process_tasks_sequential(
                    db=db,
                    dispatch_service=dispatch_service,
                    registry=registry,
                    channels=channels,
                    worker_id=worker_id,
                )
        except (SystemExit, KeyboardInterrupt):
            db.rollback()
            self._health_state = (False, {})
            logger.info("Shutting down ...")

            # Shutdown the executor if it was created
            if executor is not None:
                logger.info("Shutting down thread pool executor...")
                executor.shutdown(wait=True, cancel_futures=False)
                logger.info("Thread pool executor shutdown complete")

            self._worker_update_shutdown_event.set()
            worker_update_thread.join(5)
            if metrics_server_thread is not None:
                # set a threading event, waits until server is shutdown
                # serve the ongoing requests
                self._metrics_server_shutdown()
                metrics_server_thread.join(1)

        worker.state = models.WorkerState.SHUTDOWN
        db.add(worker)
        task_count = work_service.reschedule_dead_tasks([worker.id])
        logger.info("Reschedule %s tasks", task_count)
        dispatch_service.notify(channels)
        db.commit()

        logger.info("Shutdown gracefully")
