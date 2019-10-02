"""Dump JSON to files."""

import datetime

import json
import os
import redis as redis_sync
from sanic.log import logger

from .manager import TIMESTAMP_FORMAT

FILENAME = "{}-data.dump"


class Dumper:
    """Take JSON, write file."""

    def __init__(self, path, file_lock_time):
        if not os.path.isdir(path):
            logger.error(
                "dump-path '{}' does not exist or is not a directory.".format(path)
            )
            exit(1)
        self.lock_file = None
        self.path = path
        self.lock_time = datetime.timedelta(seconds=file_lock_time)

        # remove old entries from redis
        conn = redis_sync.Redis()
        conn.delete("dumper_lock_file")
        conn.delete("dumper_open_file")
        conn.delete("dumper_open_time")

    def __del__(self):
        """
        Destruct a dumper.

        Remove any lock files.
        """
        if self.lock_file is not None:
            os.remove(self.lock_file)

    async def _new_file(self, redis):
        # Unlock the old file
        lock_file = await redis.execute("get", "dumper_lock_file")
        if lock_file is not None:
            os.remove(lock_file)

        # Create new file
        now = datetime.datetime.utcnow().strftime(TIMESTAMP_FORMAT)
        await redis.execute("set", "dumper_open_time", now)
        open_file = os.path.join(self.path, FILENAME.format(now))
        lock_file = "{}.lock".format(open_file)
        self.lock_file = lock_file
        await redis.execute("set", "dumper_open_file", open_file)
        await redis.execute("set", "dumper_lock_file", lock_file)
        open(lock_file, "x").close()
        open(open_file, "x").close()

        logger.debug("Created new file: {}".format(open_file))

    async def dump(self, data, redis, lock_manager):
        """
        Dump json to file.

        Parameters
        ----------
        data : json
            JSON object to dump.
        """
        # Make sure the dump has a timestamp
        if "time" not in data.keys():
            data["time"] = datetime.datetime.utcnow().strftime(TIMESTAMP_FORMAT)

        async with await lock_manager.lock("dumper"):
            open_time = await redis.execute("get", "dumper_open_time")
            if open_time is None:
                await self._new_file(redis)
            else:
                open_time = datetime.datetime.strptime(open_time, TIMESTAMP_FORMAT)
                if datetime.datetime.utcnow() - open_time > self.lock_time:
                    await self._new_file(redis)

            open_file = await redis.execute("get", "dumper_open_file")

            # Dump to file
            with open(open_file, "a") as dump_file:
                json.dump(data, dump_file)
                dump_file.write("\n")
                dump_file.flush()
