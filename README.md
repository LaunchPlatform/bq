# BeanQueue [![CircleCI](https://dl.circleci.com/status-badge/img/gh/LaunchPlatform/bq/tree/master.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/gh/LaunchPlatform/beanhub-extract/tree/master)

BeanQueue, a lightweight Python task queue framework based on [SQLAlchemy](https://www.sqlalchemy.org/), PostgreSQL [SKIP LOCKED queries](https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5/) and [NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html) / [LISTEN](https://www.postgresql.org/docs/current/sql-listen.html) statements.

**Notice**: Still in its early stage, we built this for [BeanHub](https://beanhub.io)'s internal usage. May change rapidly. Use at your own risk for now.

## Features

- **Super lightweight**: Under 1K lines
- **Easy-to-deploy**: Only relies on PostgreSQL
- **Transactional**: Commit your tasks with other database entries altogether without worrying about data inconsistencies
- **Easy-to-use**: Built-in command line tools for processing tasks and helpers for generating task models
- **Auto-notify**: Automatic generation of NOTIFY statements for new or updated tasks, ensuring fast task processing
- **Retry**: Built-in and customizable retry policies
- **Schedule**: Schedule tasks to run later
- **Worker heartbeat and auto-reschedule**: Each worker keeps updating heartbeat, if one is found dead, the others will reschedule the tasks
- **Customizable**: Custom Task, Worker and Event models. Use it as a library and build your own work queue

## Install

```bash
pip install beanqueue
```

## Usage

You can define a basic task processor like this

```python
from sqlalchemy.orm import Session

import bq
from .. import models
from .. import image_utils

app = bq.BeanQueue()

@app.processor(channel="images")
def resize_image(db: Session, task: bq.Task, width: int, height: int):
    image = db.query(models.Image).filter(models.Image.task == task).one()
    image_utils.resize(image, size=(width, height))
    db.add(image)
    # by default the `processor` decorator has `auto_complete` flag turns on,
    # so it will commit the db changes for us automatically
```

The `db` and `task` keyword arguments are optional.
If you don't need to access the task object, you can simply define the function without these two parameters.
We also provide an optional `savepoint` argument in case if you want to rollback database changes you made.

To submit a task, you can either use `bq.Task` model object to construct the task object, insert into the
database session and commit.

```python
import bq
from .db import Session
from .. import models

db = Session()
task = bq.Task(
    channel="files",
    module="my_pkgs.files.processors",
    name="upload_to_s3_for_backup",
)
file = models.File(
    task=task,
    blob_name="...",
)
db.add(task)
db.add(file)
db.commit()
```

Or, you can use the `run` helper like this:

```python
from .processors import resize_image
from .db import Session
from .. import my_models

db = Session()
# a Task model generated for invoking resize_image function
task = resize_image.run(width=200, height=300)
# associate task with your own models
image = my_models.Image(task=task, blob_name="...")
db.add(image)
# we have Task model SQLALchemy event handler to send NOTIFY "<channel>" statement for you,
# so that the workers will be woken up immediately
db.add(task)
# commit will make the task visible to worker immediately
db.commit()
```

To run the worker, you can do this:

```bash
BQ_PROCESSOR_PACKAGES='["my_pkgs.processors"]' bq process images
```

The `BQ_PROCESSOR_PACKAGES` is a JSON list contains the Python packages where you define your processors (the functions you decorated with `bq.processors.registry.processor`).
To submit a task for testing purpose, you can do

```bash
bq submit images my_pkgs.processors resize_image -k '{"width": 200, "height": 300}'
```

To create tables for BeanQueue, you can run

```bash
bq create_tables
```

### Schedule

In most cases, a task will be executed as soon as possible after it is created.
To run a task later, you can set a datetime value to the `scheduled_at` attribute of the task model.
For example:

```python
import datetime

db = Session()
task = resize_image.run(width=200, height=300)
task.scheduled_at = func.now() + datetime.timedelta(minutes=3)
db.add(task)
```

Please note that currently, workers won't wake up at the next exact moment when the scheduled tasks are ready to run.
It has to wait until the polling times out, and eventually, it will see the task's scheduled_at time exceeds the current datetime.
Therefore, depending on your `POLL_TIMEOUT` setting and the number of your workers when they started processing, the actual execution may be inaccurate.
If you set the `POLL_TIMEOUT` to 60 seconds, please expect less than 60 seconds of delay.

### Retry

To automatically retry a task after failure, you can specify a retry policy to the processor.

```python
import datetime
import bq
from sqlalchemy.orm import Session

app = bq.BeanQueue()
delay_retry = bq.DelayRetry(delay=datetime.timedelta(seconds=120))

@app.processor(channel="images", retry_policy=delay_retry)
def resize_image(db: Session, task: bq.Task, width: int, height: int):
    # resize image here ...
    pass
```

Currently, we provide some simple common retry policies such as `DelayRetry` and `ExponentialBackoffRetry`.
You can define your retry policy easily by making a function that returns an optional object at the next scheduled time for retry.

```python
def my_retry_policy(task: bq.Task) -> typing.Any:
    # Calculate delay based on task model ...
    return func.now() + datetime.timedelta(seconds=delay)
```

To cap how many attempts are allowed, you can also use `LimitAttempt` like this:

```python
delay_retry = bq.DelayRetry(delay=datetime.timedelta(seconds=120))
capped_delay_retry = bq.LimitAttempt(3, delay_retry)

@app.processor(channel="images", retry_policy=capped_delay_retry)
def resize_image(db: Session, task: bq.Task, width: int, height: int):
    # Resize image here ...
    pass
```

You can also retry only for specific exception classes with the `retry_exceptions` argument.

```python
@app.processor(
    channel="images",
    retry_policy=delay_retry,
    retry_exceptions=ValueError,
)
def resize_image(db: Session, task: bq.Task, width: int, height: int):
    # resize image here ...
    pass
```

### Configurations

Configurations can be modified by setting environment variables with `BQ_` prefix.
For example, to set the python packages to scan for processors, you can set `BQ_PROCESSOR_PACKAGES`.
To change the PostgreSQL database to connect to, you can set `BQ_DATABASE_URL`.
The complete definition of configurations can be found at the [bq/config.py](bq/config.py) module.

If you want to configure BeanQueue programmatically, you can pass in `Config` object to the `bq.BeanQueue` object when creating.
For example:

```python
import bq
from .my_config import config

config = bq.Config(
    PROCESSOR_PACKAGES=["my_pkgs.processors"],
    DATABASE_URL=config.DATABASE_URL,
    BATCH_SIZE=10,
)
app = bq.BeanQueue(config=config)
```

Then you can pass `--app` argument (or `-a` for short) pointing to the app object to the process command like this:

```bash
bq -a my_pkgs.bq.app process images
```

Or if you prefer to define your own process command, you can also call `process_tasks` of the `BeanQueue` object directly like this:

```python
app.process_tasks(channels=("images",))
```

### Define your own tables

BeanQueue is designed to be as customizable as much as possible.
One of its key features is that you can define your own SQLAlchemy model instead of using the ones we provided.

To make defining your own `Task`, `Worker` or `Event` model much easier, use bq's mixin classes:

- `bq.TaskModelMixin`: provides task model columns
- `bq.TaskModelRefWorkerMixin`: provides foreign key column and relationship to `bq.Worker`
- `bq.TaskModelRefParentMixin`: provides foreign key column and relationship to children `bq.Task` created during processing
- `bq.TaskModelRefEventMixin`: provides foreign key column and relationship to `bq.Event`
- `bq.WorkerModelMixin`: provides worker model columns
- `bq.WorkerRefMixin`: provides relationship to `bq.Task`
- `bq.EventModelMixin`: provides event model columns
- `bq.EventModelRefTaskMixin`: provides foreign key column and relationship to `bq.Task`

Here's an example for defining your own Task model:

```python
import uuid

from sqlalchemy import ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
import bq
from bq.models.task import listen_events

from .base_class import Base


class Task(bq.TaskModelMixin, Base):
    __tablename__ = "task"
    worker_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("worker.id", onupdate="CASCADE"),
        nullable=True,
        index=True,
    )

    worker: Mapped["Worker"] = relationship(
        "Worker", back_populates="tasks", uselist=False
    )

listen_events(Task)
```

For task insertion and updates to notify workers, we need to register any custom task types with `bq.models.task.listen_events`.
In the example above, this is done right after the Task model definition.
For more details and advanced usage, see the definition of `bq.models.task.listen_events`.

You just see how easy it is to define your Task model. Now, here's an example for defining your own Worker model:

```python
import bq
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import relationship

from .base_class import Base


class Worker(bq.WorkerModelMixin, Base):
    __tablename__ = "worker"

    tasks: Mapped[list["Task"]] = relationship(
        "Task",
        back_populates="worker",
        cascade="all,delete",
        order_by="Task.created_at",
    )
```

With the model class ready, you only need to change the `TASK_MODEL`, `WORKER_MODEL` and `EVENT_MODEL` of `Config` to the full Python module name plus the class name like this.

```python
import bq
config = bq.Config(
    TASK_MODEL="my_pkgs.models.Task",
    WORKER_MODEL="my_pkgs.models.Worker",
    EVENT_MODEL="my_pkgs.models.Event",
    # ... other configs
)
app = bq.BeanQueue(config)
```

## Why?

There are countless work queue projects. Why make yet another one?
The primary issue with most work queue tools is their reliance on a standalone broker server.
Our work queue tasks frequently interact with the database, and the atomic nature of database transactions is great for data integrity.
However, integrating an external work queue into the system presents a risk.
The work queue and the database don't share the same data view, potentially compromising data integrity and reliability.

For example, you have a table of `images` to keep the user-uploaded images.
And you have a background work queue for resizing the uploaded images into different thumbnail sizes.
So, you will first need to insert a row for the uploaded image about the job into the database before you push the task to the work queue.

Say you push the task to the work queue immediately after you insert the `images` table then commit like this:

```
1. Insert into the "images" table
2. Push resizing task to the work queue
3. Commit db changes
```

While this might seem like the right way to do it, there's a hidden bug.
If the worker starts too fast before the transaction commits at step 3, it will not be able to see the new row in `images` as it has not been committed yet.
One may need to make the task retry a few times to ensure that even if the first attempt failed, it could see the image row in the following attempt.
But this adds complexity to the system and also increases the latency if the first attempt fails.
Also, if the commit step fails, you will have a failed work queue job trying to fetch a row from the database that will never exist.

Another approach is to push the resize task after the database changes are committed. It works like this:

```
1. Insert into the "images" table
2. Commit db changes
3. Push resizing task to the work queue
```

With this approach, we don't need to worry about workers picking up the task too early.
However, there's another drawback.
If step 3 for pushing a new task to the work queue fails, the newly inserted `images` row will never be processed.
There are many solutions to this problem, but these are all caused by inconsistent data views between the database and the work queue storage.
Things would be much easier if we had a work queue that shared the same consistent view as the database.

By using a database as the data storage, all the problems are gone.
You can simply do the following:

```
1. Insert into the "images" table
2. Insert the image resizing task into the `tasks` table
3. Commit db changes
```

It's all or nothing!
By doing so, you don't need to maintain another work queue backend.
You are probably using a database anyway, so this work queue comes for free.

Usually, a database is inefficient as the work queues data storage because of the potential lock contention and the need for constant querying.
However, things have changed since the [introduction of the SKIP LOCKED](https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5/) and [LISTEN](https://www.postgresql.org/docs/current/sql-listen.html) / [NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html) features in PostgreSQL or other databases.

This project is inspired by many of the SKIP-LOCKED-based work queue successors.
Why don't we just use those existing tools?
Well, because while they work great as work queue solutions, they don't take advantage of writing tasks and their relative data into the database in a transaction.
Many provide an abstraction function or gRPC method for pushing tasks into the database, rather than allowing users to directly insert rows and commit them together.

BeanQueue doesn't overly abstract the logic of publishing a new task into the queue.
Instead, you insert rows directly, choosing when and what to commit as tasks.

## Sponsor

<p align="center">
  <a href="https://beanhub.io"><img src="https://github.com/LaunchPlatform/bq/raw/master/assets/beanhub.svg?raw=true" alt="BeanHub logo" /></a>
</p>

A modern accounting book service based on the most popular open source version control system [Git](https://git-scm.com/) and text-based double entry accounting book software [Beancount](https://beancount.github.io/docs/index.html).

## Alternatives

- [solid_queue](https://github.com/rails/solid_queue)
- [good_job](https://github.com/bensheldon/good_job)
- [graphile-worker](https://github.com/graphile/worker)
- [postgres-tq](https://github.com/flix-tech/postgres-tq)
- [pq](https://github.com/malthe/pq/)
- [PgQueuer](https://github.com/janbjorge/PgQueuer)
- [hatchet](https://github.com/hatchet-dev/hatchet)
- [procrastinate](https://github.com/procrastinate-org/procrastinate)
