"""Condition variable using redis."""


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
    await redis.execute("hincrby", "WAITING", name, 1)

    # Wait for notification
    await redis.execute("blpop", name)

    # Decrement number of waiting processes and reset notification.
    # Script that decrements WAITING/KEY[0] and sets KEY[0] to zero if WAITING/KEY[0] is zero:
    REDIS_RESET_COND = """
    redis.call('hincrby', 'WAITING', KEY[0], -1)
    if redis.call('hget', 'WAITING', 'KEY[0]) == 0
        redis.call('set', KEY[0], 0)
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

    return await redis.execute("lpush", name, "1")
