from __future__ import annotations

import asyncio
import importlib
import logging
import platform
import sys
import typing
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version

import venusian
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine

from . import constants
from . import events
from . import models
from .config import Config
from .db.session import AsyncSessionMaker
from .metrics import MetricsServer
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
        session_cls: typing.Any = AsyncSessionMaker,
        worker_service_cls: typing.Type[WorkerService] = WorkerService,
        dispatch_service_cls: typing.Type[DispatchService] = DispatchService,
        engine: AsyncEngine | None = None,
    ):
        self.config = config if config is not None else Config()
        self.session_cls = session_cls
        self.worker_service_cls = worker_service_cls
        self.dispatch_service_cls = dispatch_service_cls
        self._engine = engine
        self._worker_update_shutdown_event: asyncio.Event = asyncio.Event()
        self._metrics_server: MetricsServer | None = None

    def create_default_engine(self) -> AsyncEngine:
        max_workers = self.config.resolved_max_concurrent_tasks()
        pool_size = max_workers + 5
        return create_async_engine(
            str(self.config.DATABASE_URL),
            pool_size=pool_size,
            max_overflow=10,
        )

    def make_session(self) -> AsyncSession:
        return self.session_cls(bind=self.engine)

    @property
    def engine(self) -> AsyncEngine:
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

    def _make_worker_service(self, session: AsyncSession) -> WorkerService:
        return self.worker_service_cls(
            session=session, task_model=self.task_model, worker_model=self.worker_model
        )

    def _make_dispatch_service(self, session: AsyncSession) -> DispatchService:
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

    async def update_workers(
        self,
        worker_id: typing.Any,
    ):
        db = self.make_session()
        try:
            worker_service = self._make_worker_service(db)
            dispatch_service = self._make_dispatch_service(db)

            current_worker = await worker_service.get_worker(worker_id)
            logger.info(
                "Updating worker %s with heartbeat_period=%s, heartbeat_timeout=%s",
                current_worker.id,
                self.config.WORKER_HEARTBEAT_PERIOD,
                self.config.WORKER_HEARTBEAT_TIMEOUT,
            )
            while True:
                dead_workers = await worker_service.fetch_dead_workers(
                    timeout=self.config.WORKER_HEARTBEAT_TIMEOUT
                )
                task_count = await worker_service.reschedule_dead_tasks(
                    [dead_worker.id for dead_worker in dead_workers]
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
                    await dispatch_service.notify(dead_worker.channels)
                if found_dead_worker:
                    await db.commit()

                await db.refresh(current_worker)
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

                try:
                    await asyncio.wait_for(
                        self._worker_update_shutdown_event.wait(),
                        timeout=self.config.WORKER_HEARTBEAT_PERIOD,
                    )
                    return
                except asyncio.TimeoutError:
                    pass

                current_worker.last_heartbeat = func.now()
                db.add(current_worker)
                await db.commit()
        finally:
            await db.close()

    async def _process_task(self, task_id: typing.Any, registry: typing.Any):
        """Process a single task with its own database session."""
        db = self.make_session()
        try:
            task = await db.get(self.task_model, task_id)
            if task is None:
                logger.error("Task %s not found", task_id)
                return
            logger.info(
                "Processing task %s, channel=%s, module=%s, func=%s",
                task.id,
                task.channel,
                task.module,
                task.func_name,
            )
            await registry.process(task, event_cls=self.event_model)
            await db.commit()
        except Exception as e:
            logger.exception("Error processing task %s: %s", task_id, e)
            await db.rollback()
            raise
        finally:
            await db.close()

    async def _process_tasks_sequential(
        self,
        db: AsyncSession,
        dispatch_service: DispatchService,
        registry: typing.Any,
        channels: tuple[str, ...],
        worker_id: typing.Any,
    ):
        """Process tasks sequentially (MAX_CONCURRENT_TASKS=1)."""
        while True:
            while True:
                tasks = await dispatch_service.dispatch(
                    channels,
                    worker_id=worker_id,
                    limit=self.config.BATCH_SIZE,
                )

                for task in tasks:
                    logger.info(
                        "Processing task %s, channel=%s, module=%s, func=%s",
                        task.id,
                        task.channel,
                        task.module,
                        task.func_name,
                    )
                    await registry.process(task, event_cls=self.event_model)
                if tasks:
                    await db.commit()

                if not tasks:
                    break

            await db.rollback()
            try:
                notifications = await dispatch_service.poll(
                    timeout=self.config.POLL_TIMEOUT
                )
                for notification in notifications:
                    logger.debug("Receive notification %s", notification)
            except TimeoutError:
                logger.debug("Poll timeout, try again")
                continue

    async def _process_tasks_concurrent(
        self,
        db: AsyncSession,
        dispatch_service: DispatchService,
        registry: typing.Any,
        channels: tuple[str, ...],
        worker_id: typing.Any,
        max_workers: int,
    ):
        """Process tasks concurrently with a semaphore, feeding new work as capacity frees."""
        running: set[asyncio.Task] = set()

        def _on_done(task: asyncio.Task):
            running.discard(task)
            try:
                task.result()
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error("Task processing failed: %s", e)

        while True:
            capacity = max_workers - len(running)
            if capacity > 0:
                tasks = await dispatch_service.dispatch(
                    channels,
                    worker_id=worker_id,
                    limit=min(capacity, self.config.BATCH_SIZE),
                )
                await db.commit()

                if tasks:
                    logger.debug(
                        "Dispatching %d tasks (running=%d, capacity=%d)",
                        len(tasks),
                        len(running),
                        capacity,
                    )
                    for task in tasks:
                        fut = asyncio.create_task(
                            self._process_task(task.id, registry),
                            name=f"task-{task.id}",
                        )
                        running.add(fut)
                        fut.add_done_callback(_on_done)

            if running:
                await asyncio.wait(
                    running,
                    timeout=0.05,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                continue

            await db.rollback()
            try:
                notifications = await dispatch_service.poll(
                    timeout=self.config.POLL_TIMEOUT
                )
                for notification in notifications:
                    logger.debug("Receive notification %s", notification)
            except TimeoutError:
                logger.debug("Poll timeout, try again")
                continue

    async def process_tasks(
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
        if not channels:
            channels = (constants.DEFAULT_CHANNEL,)

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

        db = self.make_session()
        dispatch_service = self.dispatch_service_cls(
            session=db, task_model=self.task_model
        )
        work_service = self.worker_service_cls(
            session=db, task_model=self.task_model, worker_model=self.worker_model
        )

        worker = work_service.make_worker(name=platform.node(), channels=channels)
        db.add(worker)
        await db.commit()
        await db.refresh(worker)
        await dispatch_service.listen(channels)

        if self.config.METRICS_HTTP_SERVER_ENABLED:
            self._metrics_server = MetricsServer(self, worker.id)
            await self._metrics_server.start()

        logger.info("Created worker %s, name=%s", worker.id, worker.name)
        events.worker_init.send(self, worker=worker)

        logger.info("Processing tasks in channels = %s ...", channels)
        self._worker_update_shutdown_event = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            self.update_workers(worker_id=worker.id),
            name="update_workers",
        )

        worker_id = worker.id
        max_workers = self.config.resolved_max_concurrent_tasks()
        if max_workers != 1:
            logger.info("Processing tasks concurrently with max_workers=%s", max_workers)

        try:
            if max_workers == 1:
                await self._process_tasks_sequential(
                    db=db,
                    dispatch_service=dispatch_service,
                    registry=registry,
                    channels=channels,
                    worker_id=worker_id,
                )
            else:
                await self._process_tasks_concurrent(
                    db=db,
                    dispatch_service=dispatch_service,
                    registry=registry,
                    channels=channels,
                    worker_id=worker_id,
                    max_workers=max_workers,
                )
        except (SystemExit, KeyboardInterrupt, asyncio.CancelledError):
            try:
                await db.rollback()
            except Exception:
                logger.debug("Rollback during shutdown failed", exc_info=True)
            logger.info("Shutting down ...")
        finally:
            self._worker_update_shutdown_event.set()
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except (asyncio.CancelledError, SystemExit):
                pass
            if self._metrics_server is not None:
                await self._metrics_server.shutdown()
            await dispatch_service.aclose()
            await db.close()

        async with self.make_session() as shutdown_db:
            shutdown_work = self._make_worker_service(shutdown_db)
            shutdown_dispatch = self._make_dispatch_service(shutdown_db)
            worker_row = await shutdown_work.get_worker(worker_id)
            if worker_row is not None:
                worker_row.state = models.WorkerState.SHUTDOWN
                shutdown_db.add(worker_row)
            task_count = await shutdown_work.reschedule_dead_tasks([worker_id])
            logger.info("Reschedule %s tasks", task_count)
            await shutdown_dispatch.notify(channels)
            await shutdown_db.commit()

        logger.info("Shutdown gracefully")
