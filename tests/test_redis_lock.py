import asyncio
import aioredis
import pytest

import logging
logging.basicConfig(level=logging.DEBUG)

from comet.redis_async_locks import Lock

@pytest.mark.asyncio
async def test_lock():
    """Test that the locks work when called directly."""

    redis = await aioredis.create_pool(("127.0.0.1", 6379), encoding="utf-8")
    lock = await Lock.create(redis, "test1")

    task1_end = False

    async def task1():
        r = await lock.acquire()
        await r.execute("lpush", "testlist1", 1)
        # Block until told to end
        while not task1_end:
            await asyncio.sleep(0.05)
        await lock.release()

    async def task2():
        r = await lock.acquire()
        assert await r.execute("del", "testlist1") == 1

    # Create and start up task1
    task1_future = asyncio.create_task(task1())
    await asyncio.sleep(0.2)

    # Start task2, and check that it is blocked
    task2_future = asyncio.create_task(task2())
    await asyncio.sleep(0.2)
    assert not task1_future.done()
    assert not task2_future.done()

    # Tell task1 to finish and check that both tasks finish
    task1_end = True
    await asyncio.sleep(0.2)
    assert task1_future.exception() is None
    assert task2_future.exception() is None
    assert task1_future.done()
    assert task2_future.done()

    redis.close()
    await redis.wait_closed()


@pytest.mark.asyncio
async def test_lock_manager():
    """Test that the locks work when used as a context manager."""

    redis = await aioredis.create_pool(("127.0.0.1", 6379), encoding="utf-8")
    lock = await Lock.create(redis, "test1")

    task1_end = False

    async def task1():
        async with lock as r:
            await r.execute("lpush", "testlist1", 1)
            while not task1_end:
                await asyncio.sleep(0.05)

    async def task2():
        async with lock as r:
            assert await r.execute("del", "testlist1") == 1

    # Create and start up task1
    task1_future = asyncio.create_task(task1())
    await asyncio.sleep(0.2)

    assert await lock.locked()

    # Start task2, and check that it is blocked
    task2_future = asyncio.create_task(task2())
    await asyncio.sleep(0.2)
    assert not task1_future.done()
    assert not task2_future.done()
    assert await lock.locked()

    # Tell task1 to finish and check that both tasks finish
    task1_end = True
    await asyncio.sleep(0.2)
    assert task1_future.exception() is None
    assert task2_future.exception() is None
    assert task1_future.done()
    assert task2_future.done()
    assert not await lock.locked()

    redis.close()
    await redis.wait_closed()


@pytest.mark.asyncio
async def test_large():
    """Test that the locks work when used as a context manager."""

    key = "test1"
    ntask = 256

    # Deliberately set a number of simultaneous connections much smaller than
    # the number of tasks to test connection starvation
    redis = await aioredis.create_pool(("127.0.0.1", 6379), encoding="utf-8", maxsize=1)

    # Create the lock and reset the test value
    lock = await Lock.create(redis, key)
    await redis.execute("del", key)

    # Perform a non-atomic increment of a variable in redis
    # This is obviously stupid except for testing the locking
    async def task():
        #with await redis as r:  ## Use this to see how badly the test fails
        async with lock as r:
            val = await r.execute("get", key)
            val = int(val) if val is not None else 0
            # Deliberately sleep to give other tasks a chance to break things
            # (if the lock wasn't here)
            await asyncio.sleep(0.01)
            await r.execute("set", key, val + 1)

    tasks = [task() for _ in range(ntask)]
    await asyncio.gather(*tasks)

    val = await redis.execute("get", key)
    assert int(val) == ntask

    redis.close()
    await redis.wait_closed()