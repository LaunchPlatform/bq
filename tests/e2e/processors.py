import asyncio
import datetime
import time

from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

import bq
from bq import events
from bq.processors.retry_policies import DelayRetry

app = bq.BeanQueue()

retry_once = DelayRetry(delay=datetime.timedelta(seconds=0.5))

CHANNEL = "e2e"


@events.healthz_check.connect
async def check_database(sender, worker, session: AsyncSession):
    await session.execute(text("SELECT 1"))


@app.processor(channel=CHANNEL)
def ping(n: int):
    return n


@app.processor(channel=CHANNEL)
async def async_ping(n: int):
    await asyncio.sleep(0.01)
    return n


@app.processor(channel=CHANNEL)
async def async_slow(n: int, sleep_time: float):
    start = time.time()
    await asyncio.sleep(sleep_time)
    return {"n": n, "start": start, "end": time.time()}


@app.processor(channel=CHANNEL)
async def async_db_echo(db: AsyncSession, n: int):
    now = await db.scalar(select(func.now()))
    return {"n": n, "now": now.isoformat()}


@app.processor(channel=CHANNEL)
def boom(n: int):
    raise ValueError(f"boom-{n}")


@app.processor(channel=CHANNEL, retry_policy=retry_once)
def flaky(task: bq.Task, n: int):
    if task.scheduled_at is None:
        raise ValueError(f"first-attempt-{n}")
    return n


@app.processor(channel=CHANNEL)
async def spawn_child(db: AsyncSession, n: int):
    child = leaf.run(n=n)
    db.add(child)
    return n


@app.processor(channel=CHANNEL)
async def leaf(n: int):
    return n
