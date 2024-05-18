import functools
import importlib
import logging
import platform
import sys
import threading
import time
import typing

import venusian
from sqlalchemy import func
from sqlalchemy.engine import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session as DBSession
from sqlalchemy.pool import SingletonThreadPool

from . import constants
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
        self.task_model = load_module_var(self.config.TASK_MODEL)
        self.worker_model = load_module_var(self.config.WORKER_MODEL)
        self.session_cls = session_cls
        self.worker_service_cls = worker_service_cls
        self.dispatch_service_cls = dispatch_service_cls
        if engine is None:
            engine = self.create_default_engine()
        self.engine = engine

    def create_default_engine(self):
        return create_engine(
            str(self.config.DATABASE_URL), poolclass=SingletonThreadPool
        )

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
        db = self.session_cls(bind=self.engine)

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
                    "Current worker %s state is %s instead of running, quit processing"
                )
                sys.exit(0)

            time.sleep(self.config.WORKER_HEARTBEAT_PERIOD)
            current_worker.last_heartbeat = func.now()
            db.add(current_worker)
            db.commit()

    def process_tasks(
        self,
        channels: tuple[str, ...],
    ):
        db = self.session_cls(bind=self.engine)
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
                        "  Processor module %r, processor %r", module, processor.name
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

        logger.info("Created worker %s, name=%s", worker.id, worker.name)
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

        worker.state = models.WorkerState.SHUTDOWN
        db.add(worker)
        task_count = self.worker_service_cls.reschedule_dead_tasks([worker.id])
        logger.info("Reschedule %s tasks", task_count)
        dispatch_service.notify(channels)
        db.commit()

        logger.info("Shutdown gracefully")
