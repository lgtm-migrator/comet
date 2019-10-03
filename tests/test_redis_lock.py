import asyncio
import aioredis
import pytest

from comet.redis_async_locks import (
    redis_lock_create,
    redis_lock_acquire,
    redis_lock_release,
    Lock,
)


async def acquire(name):
    red = await aioredis.create_redis(("127.0.0.1", 6379), encoding="utf-8")
    await redis_lock_acquire(red, name)
    red.close()
    await red.wait_closed()


async def release(name):
    red = await aioredis.create_redis(("127.0.0.1", 6379), encoding="utf-8")
    await redis_lock_release(red, name)
    red.close()
    await red.wait_closed()


@pytest.mark.asyncio
async def test_lock():
    name = "a"

    red = await aioredis.create_redis(("127.0.0.1", 6379), encoding="utf-8")
    await redis_lock_create(red, name)

    await asyncio.sleep(1)

    waiter = list()
    for i in range(2):
        waiter.append(asyncio.create_task(acquire(name)))
    await asyncio.sleep(1)
    last = 0
    if waiter[0].done():
        last = 1
        assert waiter[last].done() is False
    else:
        assert waiter[1].done()

    await release(name)

    assert waiter[last].done()

    await release(name)

    # trivial test of asynchronous context manager lock
    name = "c"
    await redis_lock_create(red, name)
    async with Lock(red, name) as l_c:
        print("I just locked {}".format(name))

    red.close()
    await red.wait_closed()
