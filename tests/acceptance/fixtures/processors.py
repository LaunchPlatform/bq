from bq import models
from bq.processors.registry import processor


@processor(channel="acceptance-tests")
def sum(task: models.Task, num_0: int, num_1: int):
    return num_0 + num_1
