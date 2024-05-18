import bq

app = bq.BeanQueue()


@app.processor(channel="acceptance-tests")
def sum(task: bq.Task, num_0: int, num_1: int):
    return num_0 + num_1
