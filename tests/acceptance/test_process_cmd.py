import datetime
import time
from multiprocessing import Process

from sqlalchemy.orm import Session

from .fixtures.processors import app
from .fixtures.processors import sum
from bq import models
from bq.config import Config


def run_process_cmd(db_url: str):
    app.config = Config(
        PROCESSOR_PACKAGES=["tests.acceptance.fixtures.processors"],
        DATABASE_URL=db_url,
    )
    app.process_tasks(channels=("acceptance-tests",))


def test_process_cmd(db: Session, db_url: str):
    procs = []
    for _ in range(10):
        proc = Process(target=run_process_cmd, args=(db_url,))
        proc.start()
        procs.append(proc)

    task_count = 1000
    for i in range(task_count):
        task = sum.run(num_0=i, num_1=i * 3)
        db.add(task)
    db.commit()

    begin = datetime.datetime.now()
    while True:
        done_tasks = (
            db.query(models.Task)
            .filter(models.Task.state == models.TaskState.DONE)
            .count()
        )
        if done_tasks == task_count:
            break
        delta = datetime.datetime.now() - begin
        if delta.total_seconds() > 30:
            raise TimeoutError("Timeout waiting for all tasks to finish")
        time.sleep(1)

    for proc in procs:
        proc.kill()
        proc.join(3)
