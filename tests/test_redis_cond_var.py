import asyncio
import aioredis
import pytest

from comet.redis_cond_variable import redis_condition_wait, redis_condition_notify


async def wait(name):
    red = await aioredis.create_redis(("127.0.0.1", 6379), encoding="utf-8")
    await redis_condition_wait(red, name)
    red.close()
    await red.wait_closed()
    del red


async def notify(name):
    red = await aioredis.create_redis(("127.0.0.1", 6379), encoding="utf-8")
    await redis_condition_notify(red, name)
    red.close()
    await red.wait_closed()
    del red


@pytest.mark.asyncio
async def test_cond_variable():
    name = "a"

    waiter = list()
    for i in range(20):
        waiter.append(asyncio.create_task(wait(name)))
    await asyncio.sleep(1)
    for w in waiter:
        assert w.done() is False

    await notify(name)

    for w in waiter:
        done, pending = await asyncio.wait({w})
        assert w in done
        assert w.done() is True
        await w


@pytest.mark.asyncio
async def test_cond_two_variables():
    name_a = "a"
    name_b = "b"

    waiter_a = list()
    for i in range(20):
        waiter_a.append(asyncio.create_task(wait(name_a)))

    waiter_b = list()
    for i in range(20):
        waiter_b.append(asyncio.create_task(wait(name_b)))

    await asyncio.sleep(1)

    for w in waiter_a:
        assert w.done() is False

    await notify(name_b)

    for w in waiter_a:
        assert w.done() is False

    for w in waiter_b:
        done, pending = await asyncio.wait({w})
        assert w in done
        assert w.done() is True
        await w

    for w in waiter_a:
        assert w.done() is False

    await notify(name_a)

    for w in waiter_a:
        done, pending = await asyncio.wait({w})
        assert w in done
        assert w.done() is True
        await w