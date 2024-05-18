import importlib
import typing


def get_model_class(name: str) -> typing.Type:
    module_name, model_name = name.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, model_name)
