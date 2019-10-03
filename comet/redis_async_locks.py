"""Condition variable using redis."""


class LockError(Exception):
    """An error that happened while using a lock."""

    def __init__(self, message: str):
        """
        Error with sync redis lock.

        Parameters
        ----------
        message : str
            Description of the error.
        """
        self.message = message


class Lock:
    """Async context manager for redis lock."""

    def __init__(self, redis, name):
        """Create context manager."""
        self.name = name
        self.redis = redis

    async def __aenter__(self):
        """Acquire lock."""
        await redis_lock_acquire(self.redis, self.name)

    async def __aexit__(self, exc_type, exc, tb):
        """Release lock."""
        await redis_lock_release(self.redis, self.name)


async def redis_lock_create(redis, name):
    """
    Create a lock.

    Parameters
    ----------
    redis : An aioredis connection or pool.
    name : str
        Name of the lock.

    Raises
    ------
    LockError : If the name of the lock was already taken on creation.
    """
    name = "lock_{}".format(name)

    # clear the lock
    await redis.execute("del", name)

    if (await redis.execute("lpush", name, 1)) != 1:
        raise LockError("Failure creating redis lock: {} (already used?)".format(name))


async def redis_lock_acquire(redis, name):
    """
    Acquire a lock.

    Parameters
    ----------
    redis : An aioredis connection or pool.
    name : str
        Name of the lock.

    Raises
    ------
    LockError : If the lock stored in redis has an unexpected value.
    """
    name = "lock_{}".format(name)

    if (await redis.execute("blpop", name, 0)) != [name, "1"]:
        raise LockError(
            "Failure acquiring lock: {} (unexpected value in redis lock)".format(name)
        )


async def redis_lock_release(redis, name):
    """
    Release a lock.

    Parameters
    ----------
    redis : An aioredis connection or pool.
    name : str
        Name of the lock.

    Raises
    ------
    LockError : If the lock was released already.
    """
    name = "lock_{}".format(name)

    if (await redis.execute("lpush", name, 1)) != 1:
        raise LockError("Failure releasing lock: {} (released twice?)".format(name))


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
    # if waiting[name] > 0
    #     name = 1
    REDIS_NOTIFY_COND = """
        if redis.call('hget', 'WAITING', KEYS[1]) ~= 0 then
            redis.call('lpush', KEYS[1], "1")
        end
        """
    await redis.execute("eval", REDIS_NOTIFY_COND, 1, name)
