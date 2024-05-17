# BeanQueue
BeanQueue, lightweight worker queue framework based on [SQLAlchemy](https://www.sqlalchemy.org/) and [PostgreSQL SKIP LOCKED queries](https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5/)

## Features

- **Super lightweight**: Under 1K lines
- **Easy-to-deploy**: Only rely on PostgreSQL
- **Easy-to-use**: Provide command line tools for processing tasks
- **Customizable**: Use it as an library and build your own worker queue
- **Native DB operations**: Commit your tasks with other db entries altogether without worrying about data inconsistent issue

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
Instead, we open it up to let the user insert the row and choose when and what to commit to the task

## Alternatives

TODO:
