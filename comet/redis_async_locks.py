"""Condition variable using redis."""
import logging

import asyncio
import aioredis

logger = logging.getLogger(__name__)


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
    """Async context manager for redis lock.

    This lock has a similar API to `asyncio.Lock`. The main difference is
    that acquiring the lock returns an exclusive `redis` connection for use
    by the locking task. As this is an open and active connection it can and
    should be used within the locked region to prevent other tasks starving
    the locking task of connections preventing it from unlocking the region.

    The lock requires a minimum of one connection to function.

    Parameters
    ----------
    redis : aioredis.ConnectionsPool
        A connections pool instance that will be used to connect to the redis database.
    name : str
        A name for the lock. This must be unique (i.e. not clash with
        other locks), and be set across all processes that want to use
        the same lock.
    """

    def __init__(self, redis, name):
        """Create context manager.
        """
        self.name = name
        self.redis = redis
        self._redis_conn = None

    @classmethod
    async def create(cls, redis, name):

        self = cls(redis, name)

        # Clear the lock and recreate
        await redis.execute("del", self.lockname)
        if (await redis.execute("lpush", self.lockname, 1)) != 1:
            raise LockError(f"Failure creating redis lock: {self.name} (already used?)")

        return self

    @property
    def lockname(self):
        # TODO: mangle to avoid name clashes
        return f"lock_{self.name}"

    # TODO: can we do this synchronously?
    async def locked(self):
        """Is the lock already acquired?"""
        return int(await self.redis.execute("llen", self.lockname)) == 0

    async def acquire(self, r=None):
        """Acquire the lock.

        Parameters
        ----------
        r : aioredis.Connection, optional
            A pre-existing connection to the redis database. If not set, one
            will be claimed from the pool (default). This can be used to
            re-use connections. Warning, if this happens you almost certainly
            want to use `.release(close=False)` to ensure this isn't closed by
            the lock when it is released.

        Returns
        -------
        r : aioredis.Connection
            A connection to use for accessing redis while in the locked
            region. This in a exclusive connection to guarantee that the
            worker holding the lock cannot be starved of access by other
            workers using all the redis connections.
        """
        # Acquire a connection to perform the blocking operation on the database
        logger.debug(f"Acquiring lock {self.name}: getting database conn")
        if r is None:
            r = await self.redis.acquire()
        logger.debug(f"Acquiring lock {self.name}: popping list entry")
        if (await r.execute("blpop", self.lockname, 0)) != [self.lockname, "1"]:
            raise LockError(
                f"Failure acquiring lock: {self.name} (unexpected value in redis lock)"
            )

        logger.debug(f"Acquiring lock {self.name}: setting internal connection")
        # Check there is no active connection (there shouldn't be, this is just a consistency check)
        if self._redis_conn is not None:
            raise LockError(
                f"Failure acquiring lock: {self.name} (connection not cleared)"
            )

        # Now we hold the lock, we can set the internal connection copy
        self._redis_conn = r
        return r

    async def release(self, close=True):
        """Release the lock.

        Parameters
        ----------
        close : boolean, optional
            If True, the redis connection being used by the lock currently is
            closed (default). This should be True if the connection was
            opened by the lock itself, but can be useful to set to False for
            better connection management.
        """

        # Check we have an active connection
        logger.debug(f"Releasing lock {self.name}: resetting internal connection")
        if (
            not isinstance(self._redis_conn, aioredis.connection.RedisConnection)
            or self._redis_conn.closed
        ):
            raise LockError(
                f"Failure releasing lock: {self.name} (no active redis connection)."
            )

        # Change the internal connection *before* releasing the lock in redis,
        # but keep a reference so that we can still use it in here
        r = self._redis_conn
        self._redis_conn = None

        # Release the lock in redis
        logger.debug(f"Releasing lock {self.name}: pushing new list entry")
        if (await r.execute("lpush", self.lockname, 1)) != 1:
            raise LockError(f"Failure releasing lock: {self.name} (released twice?)")

        # Close our copy of the connection
        logger.debug(f"Releasing lock {self.name}: releasing connection")
        if close:
            self.redis.release(r)

    async def __aenter__(self):
        """Acquire lock."""
        return await self.acquire()

    async def __aexit__(self, exc_type, exc, tb):
        """Release lock."""
        await self.release()


class Condition:
    """A redis condition variable.

    This aims to have a similar API to the `asyncio.Condition` except that it
    only supports `notify_all`. Additionally the `.acquire()` or context
    manager will return an exclusive redis connection for use by the locking
    task. Note that in although a `.wait()` call will release and re-acquire
    the lock, the redis connection from the lock will remain valid after the
    `.wait()` has returned.

    This requires a minimum of one available connection in the pool per
    waiting task, and one extra for the notify call as it needs to acquire
    the lock itself.
    """

    def __init__(self, lock, name):
        self.lock = lock
        self.name = name

        self.locked = lock.locked
        self.acquire = lock.acquire
        self.release = lock.release

    async def __aenter__(self):
        """Acquire lock."""
        return await self.lock.acquire()

    async def __aexit__(self, exc_type, exc, tb):
        """Release lock."""
        await self.lock.release()

    @classmethod
    async def create(cls, lock, name):
        """Create a condition variable.

        Parameters
        ----------
        lock : Lock
            A lock instance.
        name : str
            Name of the condition variable.
        """
        self = cls(lock, name)
        await self.lock.redis.execute("hset", "WAITING", self.condname, 0)
        return self

    @property
    def condname(self):
        return f"cond_{self.name}"

    @property
    def redis(self):
        return self.lock.redis

    async def notify(self, n=1):
        raise NotImplementedError(
            "only notify_all is supported for a redis condition variable."
        )

    async def notify_all(self):
        """Notify all processes waiting for the condition variable.
        """

        #
        # PSEUDOCODE:
        #
        # if waiting[name] > 0
        #     name.append(1)  # Appends to a list called name
        redis_notify_cond = """
if redis.call('hget', 'WAITING', KEYS[1]) ~= 0 then
    redis.call('lpush', KEYS[1], "1")
end
        """
        if not await self.locked():
            raise LockError(
                f"Failure notifying condition {self.name}: lock not acquired."
            )

        # Use the internal redis connection
        await self.lock._redis_conn.execute("eval", redis_notify_cond, 1, self.condname)

    async def wait(self):
        """Wait for the condition variable to signal."""

        if not await self.locked():
            raise LockError(
                f"Failure waiting condition {self.name}: lock not acquired at start."
            )

        # Save a reference to the connection so that we can preserve it through the release/acquire cycle
        r = self.lock._redis_conn

        # register as a waiting process
        #
        # PSEUDOCODE:
        #
        # waiting = dict()
        # waiting[name] += 1
        await r.execute("hincrby", "WAITING", self.condname, 1)

        # release the lock while waiting
        await self.lock.release(close=False)

        # Wait for notification
        #
        # PSEUDOCODE:
        #
        # while(True):
        #     if name:
        #         name.pop()
        await r.execute("blpop", self.condname, 0)

        # reacquire the lock
        await self.lock.acquire(r)

        # Decrement number of waiting processes and reset notification.
        # Script that decrements WAITING/KEY[0] and sets KEY[0] to zero if WAITING/KEY[0] is zero:
        #
        # PSEUDOCODE:
        #
        # waiting[name] -= 1
        # if waiting[name] > 0:
        #     name.append(1)
        redis_reset_cond = """
        if redis.call('hincrby', 'WAITING', KEYS[1], -1) ~= 0 then
            redis.call('lpush', KEYS[1], "1")
        end
        """
        await r.execute("eval", redis_reset_cond, 1, self.condname)
