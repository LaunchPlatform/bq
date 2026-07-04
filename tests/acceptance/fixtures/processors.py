import bq

from sqlalchemy.orm import Session

app = bq.BeanQueue()


@app.processor(channel="acceptance-tests")
def sum(task: bq.Task, num_0: int, num_1: int):
    return num_0 + num_1


from bq.events import healthz_check

@healthz_check.connect
def my_custom_health(sender: bq.BeanQueue, session: Session, worker: bq.models.Worker):
    print("@"*10, sender, worker)
