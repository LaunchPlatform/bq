"""Advanced unit tests for async engine and session factory."""

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

import bq
from bq.config import Config


def test_engine_recreation_with_different_config(db_url: str):
    config1 = Config(DATABASE_URL=db_url, MAX_CONCURRENT_TASKS=1)
    app1 = bq.BeanQueue(config=config1)
    engine1 = app1.engine

    config2 = Config(DATABASE_URL=db_url, MAX_CONCURRENT_TASKS=4)
    app2 = bq.BeanQueue(config=config2)
    engine2 = app2.engine

    assert engine1 is not engine2
    assert engine1.sync_engine.pool.size() == 6
    assert engine2.sync_engine.pool.size() == 9


def test_batch_size_independent_of_concurrency(db_url: str):
    config = Config(
        DATABASE_URL=db_url,
        MAX_CONCURRENT_TASKS=4,
        BATCH_SIZE=20,
    )
    app = bq.BeanQueue(config=config)

    assert app.config.MAX_CONCURRENT_TASKS == 4
    assert app.config.BATCH_SIZE == 20


def test_zero_max_workers_pool_size_calculation(db_url: str):
    config = Config(DATABASE_URL=db_url, MAX_CONCURRENT_TASKS=0)
    app = bq.BeanQueue(config=config)
    assert app.engine.sync_engine.pool.size() == 15


def test_large_concurrency_pool_size(db_url: str):
    config = Config(DATABASE_URL=db_url, MAX_CONCURRENT_TASKS=32)
    app = bq.BeanQueue(config=config)

    assert app.engine.sync_engine.pool.size() == 37
    assert app.engine.sync_engine.pool._max_overflow == 10


def test_session_factory_uses_engine_pool(db_url: str):
    config = Config(DATABASE_URL=db_url, MAX_CONCURRENT_TASKS=8)
    app = bq.BeanQueue(config=config)

    session1 = app.make_session()
    session2 = app.make_session()
    session3 = app.make_session()

    assert session1.bind is app.engine
    assert session2.bind is app.engine
    assert session3.bind is app.engine


def test_config_validation_max_concurrent_tasks():
    assert Config(MAX_CONCURRENT_TASKS=0).MAX_CONCURRENT_TASKS == 0
    assert Config(MAX_CONCURRENT_TASKS=1).MAX_CONCURRENT_TASKS == 1
    assert Config(MAX_CONCURRENT_TASKS=100).MAX_CONCURRENT_TASKS == 100


def test_pool_size_overflow_configuration(db_url: str):
    for max_workers in [2, 4, 8, 16]:
        config = Config(DATABASE_URL=db_url, MAX_CONCURRENT_TASKS=max_workers)
        app = bq.BeanQueue(config=config)
        pool = app.engine.sync_engine.pool
        assert pool._max_overflow == 10
        assert pool.size() == max_workers + 5


@pytest.mark.parametrize(
    "max_workers,batch_size,expected_relationship",
    [
        (1, 1, "equal"),
        (4, 4, "equal"),
        (4, 8, "batch_larger"),
        (4, 2, "batch_smaller"),
        (8, 16, "batch_larger"),
        (0, 10, "default_auto"),
    ],
)
def test_batch_size_concurrency_combinations(
    db_url: str, max_workers: int, batch_size: int, expected_relationship: str
):
    config = Config(
        DATABASE_URL=db_url,
        MAX_CONCURRENT_TASKS=max_workers,
        BATCH_SIZE=batch_size,
    )
    app = bq.BeanQueue(config=config)

    assert app.config.MAX_CONCURRENT_TASKS == max_workers
    assert app.config.BATCH_SIZE == batch_size

    if expected_relationship == "equal":
        assert app.config.BATCH_SIZE == app.config.MAX_CONCURRENT_TASKS
    elif expected_relationship == "batch_larger":
        assert app.config.BATCH_SIZE > app.config.MAX_CONCURRENT_TASKS
    elif expected_relationship == "batch_smaller":
        assert app.config.BATCH_SIZE < app.config.MAX_CONCURRENT_TASKS


def test_multiple_app_instances_independent_pools(db_url: str):
    app1 = bq.BeanQueue(config=Config(DATABASE_URL=db_url, MAX_CONCURRENT_TASKS=2))
    app2 = bq.BeanQueue(config=Config(DATABASE_URL=db_url, MAX_CONCURRENT_TASKS=4))
    app3 = bq.BeanQueue(config=Config(DATABASE_URL=db_url, MAX_CONCURRENT_TASKS=1))

    assert app1.engine is not app2.engine
    assert app2.engine is not app3.engine
    assert app1.engine is not app3.engine

    assert app1.engine.sync_engine.pool.size() == 7
    assert app2.engine.sync_engine.pool.size() == 9
    assert app3.engine.sync_engine.pool.size() == 6


def test_engine_caching(db_url: str):
    app = bq.BeanQueue(config=Config(DATABASE_URL=db_url, MAX_CONCURRENT_TASKS=4))

    engine1 = app.engine
    engine2 = app.engine
    engine3 = app.engine

    assert engine1 is engine2
    assert engine2 is engine3


def test_custom_engine_override(db_url: str):
    custom_engine = create_async_engine(db_url, pool_size=20)

    app = bq.BeanQueue(
        config=Config(DATABASE_URL=db_url, MAX_CONCURRENT_TASKS=1),
        engine=custom_engine,
    )

    assert app.engine is custom_engine
    assert app.engine.sync_engine.pool.size() == 20
