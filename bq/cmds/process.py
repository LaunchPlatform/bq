import functools
import importlib
import logging
import platform
import threading
import time
import typing
import uuid

import click
from sqlalchemy import func
from sqlalchemy.engine import create_engine
from sqlalchemy.pool import SingletonThreadPool

from .. import models
from ..db.session import Session
from ..processors.registry import collect
from ..services.dispatch import DispatchService
from ..services.worker import WorkerService


def update_workers(
    make_session: typing.Callable[[], Session],
    worker_id: uuid.UUID,
    heartbeat_period: int,
    heartbeat_timeout: int,
):
    db: Session = make_session()
    worker_service = WorkerService(session=db)
    dispatch_service = DispatchService(session=db)
    current_worker = db.get(models.Worker, worker_id)
    logger = logging.getLogger(__name__)
    logger.info(
        "Updating worker %s with heartbeat_period=%s, heartbeat_timeout=%s",
        current_worker.id,
        heartbeat_period,
        heartbeat_timeout,
    )
    while True:
        dead_workers = worker_service.fetch_dead_workers(timeout=heartbeat_timeout)
        channels = worker_service.reschedule_dead_tasks(
            dead_workers.with_entities(models.Worker.id)
        )
        dispatch_service.notify(channels)
        for dead_worker in dead_workers:
            logger.info(
                "Found dead worker %s (name=%s), reschedule dead tasks",
                dead_worker.id,
                dead_worker.name,
            )

        time.sleep(heartbeat_period)
        # TODO: fetch dead workers and clear their processing tasks
        current_worker.last_heartbeat = func.now()
        db.add(current_worker)
        db.commit()


@click.command()
@click.argument("channels", nargs=-1)
@click.option(
    "-p",
    "--packages",
    type=str,
    help="Packages to scan for processor functions",
    required=True,
    multiple=True,
)
@click.option(
    "-l",
    "--batch-size",
    type=int,
    default=1,
    help="Size of tasks batch to fetch each time from the database",
)
@click.option(
    "--pull-timeout",
    type=int,
    default=60,
    help="How long we should poll before timeout in seconds",
)
@click.option(
    "--worker-heartbeat-period",
    type=int,
    default=30,
    help="Interval of worker heartbeat update cycle in seconds",
)
@click.option(
    "--worker-heartbeat-timeout",
    type=int,
    default=100,
    help="Timeout of worker heartbeat in seconds",
)
def main(
    channels: tuple[str, ...],
    packages: tuple[str, ...],
    batch_size: int,
    pull_timeout: int,
    worker_heartbeat_period: int,
    worker_heartbeat_timeout: int,
):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # FIXME: the uri from opt
    engine = create_engine(
        "postgresql://bq:@localhost/bq_test",
        poolclass=SingletonThreadPool,
    )
    Session.configure(bind=engine)

    logger.info("Scanning packages %s", packages)
    pkgs = list(map(importlib.import_module, packages))
    registry = collect(pkgs)
    for channel, module_processors in registry.processors.items():
        logger.info("Collected processors with channel %r", channel)
        for module, func_processors in module_processors.items():
            for processor in func_processors.values():
                logger.info(
                    "  Processor module %r, processor %r", module, processor.name
                )

    db = Session()
    dispatch_service = DispatchService(session=db)
    worker_service = WorkerService(session=db)
    worker = models.Worker(name=platform.node(), channels=channels)
    db.add(worker)
    dispatch_service.listen(channels)
    db.commit()

    logger.info("Created worker %s, name=%s", worker.id, worker.name)
    logger.info("Processing tasks in channels = %s ...", channels)

    worker_update_thread = threading.Thread(
        target=functools.partial(
            update_workers,
            make_session=Session,
            worker_id=worker.id,
            heartbeat_period=worker_heartbeat_period,
            heartbeat_timeout=worker_heartbeat_timeout,
        ),
        name="update_workers",
    )
    worker_update_thread.daemon = True
    worker_update_thread.start()

    try:
        while True:
            for task in dispatch_service.dispatch(
                channels, worker=worker, limit=batch_size
            ):
                logger.info(
                    "Processing task %s, channel=%s, module=%s, func=%s",
                    task.id,
                    task.channel,
                    task.module,
                    task.func_name,
                )
                # TODO: support processor pool and other approaches to dispatch the workload
                registry.process(task)
            # we will not see notifications in a transaction, need to close the transaction first before entering
            # polling
            db.close()
            try:
                for notification in dispatch_service.poll(timeout=pull_timeout):
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
    dispatch_service.notify(worker_service.reschedule_dead_tasks([worker.id]))
    db.commit()

    logger.info("Shutdown gracefully")


if __name__ == "__main__":
    main()
