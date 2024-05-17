# BeanQueue  [![CircleCI](https://dl.circleci.com/status-badge/img/gh/LaunchPlatform/bq/tree/master.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/gh/LaunchPlatform/beanhub-extract/tree/master)
BeanQueue, a lightweight worker queue framework based on [SQLAlchemy](https://www.sqlalchemy.org/), [PostgreSQL SKIP LOCKED queries](https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5/) and [NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html) / [LISTEN](https://www.postgresql.org/docs/current/sql-listen.html) statements.

**Notice**: Still in its early stage, we built this for [BeanHub](https://beanhub.io)'s internal usage. May change rapidly. Use at your own risk for now.

## Features

- **Super lightweight**: Under 1K lines
- **Easy-to-deploy**: Only rely on PostgreSQL
- **Easy-to-use**: Provide command line tools for processing tasks, also helpers for generating tasks models
- **Auto-notify**: Notify will automatically be generated and send for inserted or update tasks
- **Worker heartbeat and auto-reschedule**: Each worker keeps updating heartbeat, if one is dead, the others will reschedule the tasks
- **Customizable**: Use it as an library and build your own worker queue
- **Native DB operations**: Commit your tasks with other db entries altogether without worrying about data inconsistent issue

## Install

```bash
pip install beanqueue
```

## Usage

You can define a task processor like this

```python
from sqlalchemy.orm import Session

from bq.processors.registry import processor
from bq import models
from .. import my_models
from .. import image_utils

@processor(channel="images")
def resize_image(db: Session, task: models.Task, width: int, height: int):
    image = db.query(my_models.Image).filter(my_models.Image.task == task).one()
    image_utils.resize(image, size=(width, height))
    db.add(image)
    # by default the `processor` decorator has `auto_complete` flag turns on,
    # so it will commit the db changes for us automatically
```

The `db` and `task` keyword arguments are optional.
If you don't need to access the task object, you can simply define the function without these two parameters.

To submit a task, you can either use `bq.models.Task` model object to construct the task object, insert into the
database session and commit.

```python
from bq import models
from .db import Session
from .. import my_models

db = Session()
task = models.Task(
    channel="files",
    module="my_pkgs.files.processors",
    name="upload_to_s3_for_backup",
)
file = my_models.File(
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
BQ_PROCESSOR_PACKAGES='["my_pkgs.processors"]' python -m bq.cmds.process images
```

To submit a task for testing purpose, you can do

```bash
python -m bq.cmds.submit images my_pkgs.processors resize_image -k '{"width": 200, "height": 300}'
```

To create tables for BeanQueue, you can run

```bash
python -m bq.cmds.create_tables
```

### Configurations

Configurations can be modified by setting environment variables with `BQ_` prefix.
For example, to set the python packages to scan for processors, you can set `BQ_PROCESSOR_PACKAGES`.
To change the PostgreSQL database to connect to, you can set `BQ_DATABASE_URL`.
The complete definition of configurations can be found at the [bq/config.py](bq/config.py) module.
For now, the configurations only affect command line tools.

If you want to configure BeanQueue programmatically for the command lines, you can override our [dependency-injector](https://python-dependency-injector.ets-labs.org/)'s container defined at [bq/container.py](bq/container.py) and call the command function manually.
For example:

```python
import bq.cmds.process
from bq.container import Container
from bq.config import Config

container = Container()
container.wire(modules=[bq.cmds.process])
with container.config.override(
    Config(
        PROCESSOR_PACKAGES=["my_pkgs.processors"],
        DATABASE_URL="postgresql://...",
        BATCH_SIZE=10,
    )
):
    bq.cmds.process.process_tasks(channels=("images",))
```

Many other behaviors of this framework can also be modified by overriding the container defined at [bq/container.py](bq/container.py).

## Why?

There are countless worker queue projects. Why make yet another one?
The primary issue with most worker queue tools is their reliance on a standalone broker server.
Our worker queue tasks frequently interact with the database, and the atomic nature of database transactions is great for data integrity.
However, integrating an external worker queue into the system presents a risk.
The worker queue and the database don't share the same data view, potentially compromising data integrity and reliability.

For example, you have a table of `images` to keep the user-uploaded images.
And you have a background worker queue for resizing the uploaded images into different thumbnail sizes.
So, you will first need to insert a row for the uploaded image about the job into the database before you push the task to the worker queue.

Say you push the task to the worker queue immediately after you insert the `images` table then commit like this:

```
1. Insert into the "images" table
2. Push resizing task to the worker queue
3. Commit db changes
```

While this might seem like the right way to do it, there's a hidden bug.
If the worker starts too fast before the transaction commits at step 3, it will not be able to see the new row in `images` as it has not been committed yet.
One may need to make the task retry a few times to ensure that even if the first attempt failed, it could see the image row in the following attempt.
But this adds complexity to the system and also increases the latency if the first attempt fails.
Also, if the commit step fails, you will have a failed worker queue job trying to fetch a row from the database that will never exist.

Another approach is to push the resize task after the database changes are committed. It works like this:

```
1. Insert into the "images" table
2. Commit db changes
3. Push resizing task to the worker queue
```

With this approach, we don't need to worry about workers picking up the task too early.
However, there's another drawback.
If step 3 for pushing a new task to the worker queue fails, the newly inserted `images` row will never be processed.
There are many solutions to this problem, but these are all caused by inconsistent data views between the database and the worker queue storage.
Things will be much easier if we have a worker queue that shares the same consistent view with the worker queue.

By using a database as the data storage, all the problems are gone.
You can simply do the following:

```
1. Insert into the "images" table
2. Insert the image resizing task into the `tasks` table
3. Commit db changes
```

It's all or nothing!
By doing so, you don't need to maintain another worker queue backend.
You are probably using a database anyway, so this worker queue comes for free.

Usually, a database is inefficient as the worker queues data storage because of the potential lock contention and the need for constant querying.
However, things have changed since the [introduction of the SKIP LOCKED](https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5/) and [LISTEN](https://www.postgresql.org/docs/current/sql-listen.html) / [NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html) features in PostgreSQL or other databases.

This project is inspired by many of the SKIP-LOCKED-based worker queue successors.
Why don't we just use those existing tools?
Well, because while they work great as worker queue solutions, they don't take advantage of writing tasks and their relative data into the database in a transaction.
Many provide an abstraction function or gRPC method of pushing tasks into the database instead of opening it up for the user to insert the row directly with other rows and commit altogether.

With BeanQueue, we don't abstract away the logic of publishing a new task into the queue.
Instead, we open it up to let the user insert the row and choose when and what to commit to the task.

## Sponsor

<p align="center">
  <a href="https://beanhub.io"><img src="https://github.com/LaunchPlatform/bq/raw/master/assets/beanhub.svg?raw=true" alt="BeanHub logo" /></a>
</p>

A modern accounting book service based on the most popular open source version control system [Git](https://git-scm.com/) and text-based double entry accounting book software [Beancount](https://beancount.github.io/docs/index.html).

## Alternatives

- [solid_queue](https://github.com/rails/solid_queue)
- [postgres-tq](https://github.com/flix-tech/postgres-tq)
- [PgQueuer](https://github.com/janbjorge/PgQueuer)
- [hatchet](https://github.com/hatchet-dev/hatchet)
