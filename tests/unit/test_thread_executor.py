"""Unit tests for async engine configuration and pool management."""
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import AsyncAdaptedQueuePool

import bq
from bq.config import Config


def test_default_pool_is_async_adapted(db_url: str):
    app = bq.BeanQueue(
        config=Config(
            DATABASE_URL=db_url,
            MAX_CONCURRENT_TASKS=1,
        )
    )

    engine = app.engine
    assert isinstance(engine, AsyncEngine)
    assert isinstance(engine.sync_engine.pool, AsyncAdaptedQueuePool) or isinstance(
        engine.pool, AsyncAdaptedQueuePool
    )


def test_concurrent_pool_size_configuration(db_url: str):
    max_workers = 8
    app = bq.BeanQueue(
        config=Config(
            DATABASE_URL=db_url,
            MAX_CONCURRENT_TASKS=max_workers,
        )
    )

    pool = app.engine.sync_engine.pool
    expected_pool_size = max_workers + 5
    assert pool.size() == expected_pool_size
    assert pool._max_overflow == 10


def test_zero_max_workers_uses_default_pool_size(db_url: str):
    app = bq.BeanQueue(
        config=Config(
            DATABASE_URL=db_url,
            MAX_CONCURRENT_TASKS=0,
        )
    )

    pool = app.engine.sync_engine.pool
    assert pool.size() == 15


@pytest.mark.parametrize(
    "max_workers,expected_pool_size",
    [
        (1, 6),
        (2, 7),
        (4, 9),
        (8, 13),
        (16, 21),
        (0, 15),
    ],
)
def test_pool_configuration_matrix(
    db_url: str, max_workers: int, expected_pool_size: int
):
    app = bq.BeanQueue(
        config=Config(
            DATABASE_URL=db_url,
            MAX_CONCURRENT_TASKS=max_workers,
        )
    )

    pool = app.engine.sync_engine.pool
    assert pool.size() == expected_pool_size


def test_config_max_concurrent_tasks_default():
    config = Config()
    assert config.MAX_CONCURRENT_TASKS == 1
    assert config.MAX_WORKER_THREADS == 1


def test_config_max_concurrent_tasks_from_env(monkeypatch):
    monkeypatch.setenv("BQ_MAX_CONCURRENT_TASKS", "8")
    config = Config()
    assert config.MAX_CONCURRENT_TASKS == 8


def test_config_max_worker_threads_env_alias(monkeypatch):
    monkeypatch.setenv("BQ_MAX_WORKER_THREADS", "8")
    config = Config()
    assert config.MAX_CONCURRENT_TASKS == 8


def test_config_batch_size_with_concurrency():
    config = Config(
        MAX_CONCURRENT_TASKS=8,
        BATCH_SIZE=20,
    )
    assert config.MAX_CONCURRENT_TASKS == 8
    assert config.BATCH_SIZE == 20


def test_config_max_worker_threads_kwarg_alias():
    config = Config(MAX_WORKER_THREADS=4)
    assert config.MAX_CONCURRENT_TASKS == 4
    assert config.MAX_WORKER_THREADS == 4
