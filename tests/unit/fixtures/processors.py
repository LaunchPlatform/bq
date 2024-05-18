from bq import models
from bq.app import BeanQueue


app = BeanQueue()


@app.processor(channel="mock-channel")
def processor0(task: models.Task):
    return "processed by processor0"


@app.processor(channel="mock-channel2")
def processor1(task: models.Task, kwarg0: str):
    return kwarg0
