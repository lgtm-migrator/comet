"""Condition variable using redis."""


async def redis_create_lock(redis, name):
    """
    Create a lock.

    Parameters
    ----------
    redis : An aioredis connection or pool.
    name : str
        Name of the lock.

    Returns
    -------
    bool : False in case of error.
    """
    name = "lock_{}".format(name)

    # clear the lock
    await redis.execute("del", name)

    return (await redis.execute("lpush", name, 1)) == 1


async def redis_lock_acquire(redis, name):
    """
    Acquire a lock.

    Parameters
    ----------
    redis : An aioredis connection or pool.
    name : str
        Name of the lock.

    Returns
    -------
    bool : False in case of error.
    """
    name = "lock_{}".format(name)

    return (await redis.execute("blpop", name, 0)) == [name, "1"]


async def redis_lock_release(redis, name):
    """
    Release a lock.

    Parameters
    ----------
    redis : An aioredis connection or pool.
    name : str
        Name of the lock.

    Returns
    -------
    bool : False in case of error.
    """
    name = "lock_{}".format(name)

    return (await redis.execute("lpush", name, 1)) == 1


async def redis_condition_wait(redis, name):
    """
    Wait for a condition variable.

    Parameters
    ----------
    redis : :class:`aioredis.RedisConnection`
    name : str
        Name of the condition variable.
    """
    name = "cond_{}".format(name)

    # register as a waiting process
    #
    # PSEUDOCODE:
    #
    # waiting = dict()
    # waiting[name] += 1
    await redis.execute("hincrby", "WAITING", name, 1)

    # Wait for notification
    #
    # PSEUDOCODE:
    #
    # while(True):
    #     if name:
    #         name = None
    await redis.execute("blpop", name, 0)

    # Decrement number of waiting processes and reset notification.
    # Script that decrements WAITING/KEY[0] and sets KEY[0] to zero if WAITING/KEY[0] is zero:
    #
    # PSEUDOCODE:
    #
    # waiting[name] -= 1
    # if waiting[name] > 0:
    #     name = 1
    REDIS_RESET_COND = """
    if redis.call('hincrby', 'WAITING', KEYS[1], -1) ~= 0 then
        redis.call('lpush', KEYS[1], "1")
    end
    """
    await redis.execute("eval", REDIS_RESET_COND, 1, name)


async def redis_condition_notify(redis, name):
    """
    Notify all processes waiting for the condition variable.

    Parameters
    ----------
    name : str
        Name of the condition variable.
    """
    name = "cond_{}".format(name)

    #
    # PSEUDOCODE:
    #
    # name = 1
    return await redis.execute("lpush", name, "1")
