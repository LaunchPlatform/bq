from bq.processors.registry import collect


def test_collect():
    from . import fixtures

    registry = collect([fixtures])
    assert registry.keys() == {"mock-channel", "mock-channel2"}
    module_name = ".".join(__name__.split(".")[:-1]) + ".fixtures.processors"

    modules0 = registry["mock-channel"]
    assert modules0.keys() == {module_name}
    funcs0 = modules0[module_name]
    assert funcs0.keys() == {"processor0"}

    modules1 = registry["mock-channel2"]
    assert modules1.keys() == {module_name}
    funcs1 = modules1[module_name]
    assert funcs1.keys() == {"processor1"}
