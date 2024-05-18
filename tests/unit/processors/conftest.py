import pytest


@pytest.fixture
def processor_module() -> str:
    return ".".join(__name__.split(".")[:-2]) + ".fixtures.processors"
