from bq import models
from bq.processors.registry import processor


@processor(channel="mock-channel")
def processor0(task: models.Task):
    return "processed by processor0"


@processor(channel="mock-channel2")
def processor1(task: models.Task, kwarg0: str):
    return kwarg0
