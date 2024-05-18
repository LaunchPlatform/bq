import pytest
from sqlalchemy.orm import Session

from .. import fixtures
from .conftest import processor_module
from bq import models
from bq.processors.registry import collect
from bq.processors.registry import Registry


@pytest.fixture
def registry() -> Registry:
    return collect([fixtures])


def test_collect(registry: Registry, processor_module: str):
    assert registry.processors.keys() == {"mock-channel", "mock-channel2"}

    modules0 = registry.processors["mock-channel"]
    assert modules0.keys() == {processor_module}
    funcs0 = modules0[processor_module]
    assert funcs0.keys() == {"processor0"}

    modules1 = registry.processors["mock-channel2"]
    assert modules1.keys() == {processor_module}
    funcs1 = modules1[processor_module]
    assert funcs1.keys() == {"processor1"}


@pytest.mark.parametrize(
    "task__channel, task__module, task__func_name, task__kwargs, expected",
    [
        (
            "mock-channel",
            "tests.unit.fixtures.processors",
            "processor0",
            {},
            "processed by processor0",
        ),
        (
            "mock-channel2",
            "tests.unit.fixtures.processors",
            "processor1",
            dict(kwarg0="mock-val"),
            "mock-val",
        ),
    ],
)
def test_registry_process(
    db: Session, registry: Registry, task: models.Task, expected: str
):
    assert registry.process(task) == expected
