"""REST Server for CoMeT (the Dataset Broker)."""
import aioredis
import asyncio
import datetime
import json
import os
import redis as redis_sync
import time

from bisect import bisect_left
from caput import time as caput_time
from math import ceil
from socket import socket
from threading import Thread

from sanic import Sanic
from sanic import response
from sanic.log import logger
from concurrent.futures import CancelledError

from . import __version__
from .manager import Manager, CometError, TIMESTAMP_FORMAT
from .redis_async_locks import Lock, Condition, LockError

WAIT_TIME = 40
DEFAULT_PORT = 12050
REDIS_SERVER = ("localhost", 6379)

app = Sanic(__name__)
app.config.REQUEST_TIMEOUT = 600
app.config.RESPONSE_TIMEOUT = 600


@app.route("/status", methods=["GET"])
async def status(request):
    """
    Get status of CoMeT (dataset-broker).

    Poke comet to see if it's alive. Is either dead or returns {"running": True}.

    curl -X GET http://localhost:12050/status
    """
    logger.debug("status: Received status request")
    return response.json({"running": True, "result": "success"})


@app.route("/states", methods=["GET"])
async def get_states(request):
    """
    Get states from CoMeT (dataset-broker).

    Shows all states registered by CoMeT (the broker).

    curl -X GET http://localhost:12050/states
    """

    logger.debug("status: Received states request")

    states = await redis.execute("hkeys", "states")
    reply = {"result": "success", "states": states}

    logger.debug("states: {}".format(states))
    return response.json(reply)


@app.route("/datasets", methods=["GET"])
async def get_datasets(request):
    """
    Get datasets from CoMeT (dataset-broker).

    Shows all datasets registered by CoMeT (the broker).

    curl -X GET http://localhost:12050/datasets
    """
    logger.debug("status: Received datasets request")

    datasets = await redis.execute("hkeys", "datasets")
    reply = {"result": "success", "datasets": datasets}

    logger.debug("datasets: {}".format(datasets))
    return response.json(reply)


@app.route("/register-external-state", methods=["POST"])
async def external_state(request):
    """Register an external state that is detached from any dataset."""
    hash = request.json["hash"]
    type = request.json["type"]
    logger.debug("Received external state: {} with hash {}".format(type, hash))

    result = await register_state(request)

    async with lock_external_states as r:
        await r.execute("hset", "external_state", type, hash)

    # TODO: tell kotekan that this happened

    return result


@app.route("/register-state", methods=["POST"])
async def register_state(request):
    """Register a dataset state with the comet broker.

    This should only ever be called by kotekan's datasetManager.
    """
    hash = request.json["hash"]
    logger.debug(
        "register-state: Received register state request, hash: {}".format(hash)
    )
    reply = dict(result="success")

    # Lock states and check if the received state is already known.
    async with lock_states as r:
        state = await r.execute("hget", "states", hash)
        if state is None:
            # we don't know this state, did we request it already?
            if await r.execute("sismember", "requested_states", hash):
                return response.json(reply)

            # otherwise, request it now
            await r.execute("sadd", "requested_states", hash)
            reply["request"] = "get_state"
            reply["hash"] = hash
            logger.debug("register-state: Asking for state, hash: {}".format(hash))

    return response.json(reply)


@app.route("/send-state", methods=["POST"])
async def send_state(request):
    """Send a dataset state to CoMeT (the broker).

    This should only ever be called by kotekan's datasetManager.
    """
    hash = request.json["hash"]
    state = request.json["state"]
    if state:
        type = state["type"]
    else:
        type = None
    logger.debug("send-state: Received {} state {}".format(type, hash))
    reply = dict()

    # Lock states and check if we know this state already.
    async with lock_states as r:
        found = await r.execute("hget", "states", hash)
        if found is not None:
            # if we know it already, does it differ?
            if found != state:
                reply["result"] = (
                    "error: hash collision ({})\nTrying to register the following dataset state:\n{},\nbut a different state is know to "
                    "the broker with the same hash:\n{}".format(hash, state, found)
                )
                logger.warning("send-state: {}".format(reply["result"]))
            else:
                reply["result"] = "success"
        else:
            await r.execute("hset", "states", hash, json.dumps(state))
            reply["result"] = "success"
            await cond_states.notify_all()

    # Remove it from the set of requested states (if it's in there.)
    await redis.execute("srem", "requested_states", hash)

    return response.json(reply)


@app.route("/register-dataset", methods=["POST"])
async def register_dataset(request):
    """Register a dataset with CoMeT (the broker).

    This should only ever be called by kotekan's datasetManager.
    """
    hash = request.json["hash"]
    ds = request.json["ds"]
    logger.debug(
        "register-dataset: Registering new dataset with hash {} : {}".format(hash, ds)
    )
    dataset_valid = await check_dataset(ds)
    reply = dict()
    root = await find_root(hash, ds)

    # Lack datasets and check if dataset already known.
    async with lock_datasets as r:
        found = await r.execute("hget", "datasets", hash)
        if found is not None:
            # this string needs to be deserialized, contains a dataset
            found = json.loads(found)
            # if we know it already, does it differ?
            if found != ds:
                reply["result"] = (
                    "error: hash collision ({})\nTrying to register the following dataset:\n{},\nbut a different one is know to "
                    "the broker with the same hash:\n{}".format(hash, ds, found)
                )
                logger.warning("send-state: {}".format(reply["result"]))
            else:
                reply["result"] = "success"
        elif dataset_valid:
            # save the dataset
            await save_dataset(r, hash, ds, root)

            reply["result"] = "success"
            await cond_datasets.notify_all()
        else:
            reply["result"] = "Dataset {} invalid.".format(hash)
            logger.debug(
                "register-dataset: Received invalid dataset with hash {} : {}".format(
                    hash, ds
                )
            )

    return response.json(reply)


async def save_dataset(r, hash, ds, root):
    """Save the given dataset, its hash and a current timestamp.

    This should be called while a lock on the datasets is held.
    """
    # add a timestamp to the dataset (ms precision)
    ts = caput_time.datetime_to_unix(datetime.datetime.utcnow())

    # get dicts from redis concurrently
    task = asyncio.ensure_future(r.execute("hget", "datasets_of_root", root))
    datasets_of_root_keys = await r.execute("hget", "datasets_of_root_keys", root)
    (task,), _ = await asyncio.wait({task})
    datasets_of_root = task.result()

    # create entry if this is the first node with that root
    if not datasets_of_root:
        datasets_of_root = list()
        datasets_of_root_keys = list()
    else:
        datasets_of_root = json.loads(datasets_of_root)
        datasets_of_root_keys = json.loads(datasets_of_root_keys)

    # Determine where to insert dataset ID.
    i = bisect_left(datasets_of_root_keys, ts)

    # Insert timestamp in keys list.
    datasets_of_root_keys.insert(i, ts)

    # Insert the dataset ID itself in the corresponding place.
    datasets_of_root.insert(i, hash)

    # save changes
    task1 = asyncio.ensure_future(
        r.execute("hset", "datasets_of_root", root, json.dumps(datasets_of_root))
    )
    task2 = asyncio.ensure_future(
        r.execute(
            "hset", "datasets_of_root_keys", root, json.dumps(datasets_of_root_keys)
        )
    )

    # Insert the dataset in the hashmap
    task3 = asyncio.ensure_future(r.execute("hset", "datasets", hash, json.dumps(ds)))

    # Wait for all concurrent tasks
    await asyncio.wait({task1, task2, task3})


async def gather_update(ts, roots):
    """Gather the update for a given time and roots.

    Returns a dict of dataset ID -> dataset with all datasets with the
    given roots that were registered after the given timestamp.
    """
    update = dict()
    async with lock_datasets as r:
        for root in roots:
            # Get both dicts from redis concurrently:
            keys, tree = await asyncio.gather(
                r.execute("hget", "datasets_of_root_keys", root),
                r.execute("hget", "datasets_of_root", root),
            )

            keys = reversed(json.loads(keys))
            tree = list(reversed(json.loads(tree)))

            # The nodes in tree are ordered by their timestamp from new to
            # old, so we are done as soon as we find an older timestamp than
            # the given one.
            tasks = []
            for n, k in zip(tree, keys):
                if k < ts:
                    break
                tasks.append(asyncio.ensure_future(r.execute("hget", "datasets", n)))

            if tasks:
                # Wait for all concurrent tasks
                tasks, _ = await asyncio.wait(tasks)
                # put back together the root ds IDs and the datasets
                update.update(
                    dict(zip(tree, [json.loads(task.result()) for task in tasks]))
                )
    return update


async def find_root(hash, ds):
    """Return the dataset Id of the root of this dataset."""
    root = hash
    while not ds["is_root"]:
        root = ds["base_dset"]
        found = await wait_for_dset(root)
        if not found:
            logger.error("find_root: dataset {} not found.".format(hash))
            return None
        ds = json.loads(await redis.execute("hget", "datasets", root))
    return root


async def check_dataset(ds):
    """Check if a dataset is valid.

    For a dataset to be valid, the state and base dataset it references to
    have to exist. If it is a root dataset, the base dataset does not have
    to exist.
    """
    logger.debug("check_dataset: Checking dataset: {}".format(ds))
    found = await wait_for_state(ds["state"])
    if not found:
        logger.debug("check_dataset: State of dataset unknown: {}".format(ds))
        return False
    if ds["is_root"]:
        logger.debug("check_dataset: dataset {} OK".format(ds))
        return True
    found = await wait_for_dset(ds["base_dset"])
    if not found:
        logger.debug("check_dataset: Base dataset of dataset unknown: {}".format(ds))
        return False
    return True


@app.route("/request-state", methods=["POST"])
async def request_state(request):
    """Request the state with the given ID.

    This is called by kotekan's datasetManager.

    curl -d '{"state_id":42}' -X POST -H "Content-Type: application/json"
         http://localhost:12050/request-state
    """
    id = request.json["id"]

    logger.debug("request-state: Received request for state with ID {}".format(id))
    reply = dict()
    reply["id"] = id

    # Do we know this state ID?
    logger.debug("request-state: waiting for state ID {}".format(id))
    found = await wait_for_state(id)
    if not found:
        reply["result"] = "state ID {} unknown to broker.".format(id)
        logger.info("request-state: State {} unknown to broker".format(id))
        return response.json(reply)
    logger.debug("request-state: found state ID {}".format(id))

    reply["state"] = json.loads(await redis.execute("hget", "states", id))

    reply["result"] = "success"
    logger.debug("request-state: Replying with state {}".format(id))
    return response.json(reply)


async def wait_for_dset(id):
    """Wait until the given dataset is present."""
    found = True
    r = await lock_datasets.acquire()

    if not await r.execute("hexists", "datasets", id):
        # wait for half of kotekans timeout before we admit we don't have it
        await lock_datasets.release()
        logger.debug("wait_for_ds: Waiting for dataset {}".format(id))
        wait_time = WAIT_TIME
        start_wait = time.time()
        while True:
            # did someone send it to us by now?
            async with cond_datasets as r:
                try:
                    await cond_datasets.wait(wait_time)
                except TimeoutError:
                    logger.warning(
                        "wait_for_ds: Timeout ({}s) when waiting for dataset {}".format(
                            WAIT_TIME, id
                        )
                    )
                    return False
                except CancelledError:
                    logger.warning(
                        "wait_for_ds: Request cancelled while waiting for dataset {}".format(
                            id
                        )
                    )
                    return False

                if await r.execute("hexists", "datasets", id):
                    logger.debug("wait_for_ds: Found dataset {}".format(id))
                    break

                # we have to continue waiting. Count down on the wait_time.
                wait_time = int(ceil(WAIT_TIME - (time.time() - start_wait)))
                if wait_time == 0:
                    logger.warning(
                        "wait_for_ds: Timeout ({}s) when waiting for dataset {}".format(
                            WAIT_TIME, id
                        )
                    )
                    return False
        if not await redis.execute("hexists", "datasets", id):
            logger.warning(
                "wait_for_ds: Timeout ({}s) when waiting for dataset {}".format(
                    WAIT_TIME, id
                )
            )
            found = False
    else:
        await lock_datasets.release()
    return found


async def wait_for_state(id):
    """Wait until the given state is present."""
    found = True
    r = await lock_states.acquire()
    if not await r.execute("hexists", "states", id):
        # wait for half of kotekans timeout before we admit we don't have it
        await lock_states.release()
        logger.debug("wait_for_state: Waiting for state {}".format(id))
        wait_time = WAIT_TIME
        start_wait = time.time()
        while True:
            # did someone send it to us by now?
            async with cond_states as r:
                try:
                    await cond_states.wait(wait_time)
                except TimeoutError:
                    logger.warning(
                        "wait_for_ds: Timeout ({}s) when waiting for state {}".format(
                            WAIT_TIME, id
                        )
                    )
                    return False
                except CancelledError:
                    logger.warning(
                        "wait_for_ds: Request cancelled while waiting for state {}".format(
                            id
                        )
                    )
                    return False
                if await r.execute("hexists", "states", id):
                    logger.debug("wait_for_ds: Found state {}".format(id))
                    break

                # we have to continue waiting. Count down on the wait_time.
                wait_time = int(ceil(WAIT_TIME - (time.time() - start_wait)))
                if wait_time == 0:
                    logger.warning(
                        "wait_for_ds: Timeout ({}s) when waiting for state {}".format(
                            WAIT_TIME, id
                        )
                    )
                    return False
        # No lock here, cannot just use r
        if not await redis.execute("hexists", "states", id):
            logger.warning(
                "wait_for_state: Timeout ({}s) when waiting for state {}".format(
                    WAIT_TIME, id
                )
            )
            found = False
    else:
        await lock_states.release()
    return found


@app.route("/update-datasets", methods=["POST"])
async def update_datasets(request):
    """Get an update on the datasets.

    Request all nodes that where added after the given timestamp.
    If the root of the given dataset is not among the given known roots,
    All datasets with the same root as the given dataset are included in the
    returned update additionally.

    This is called by kotekan's datasetManager.

    curl
    -d '{"ds_id":2143,"ts":0,"roots":[1,2,3]}'
    -X POST
    -H "Content-Type: application/json"
    http://localhost:12050/update-datasets
    """
    ds_id = request.json["ds_id"]
    ts = request.json["ts"]
    roots = request.json["roots"]

    logger.debug(
        "update-datasets: Received request for ancestors of dataset {} since timestamp "
        "{}, roots {}.".format(ds_id, ts, roots)
    )
    reply = dict()
    reply["datasets"] = dict()

    # Do we know this ds ID?
    found = await wait_for_dset(ds_id)
    if not found:
        reply["result"] = "update-datasets: Dataset ID {} unknown to broker.".format(
            ds_id
        )
        logger.info("update-datasets: Dataset ID {} unknown.".format(ds_id))
        return response.json(reply)

    if ts is 0:
        ts = caput_time.datetime_to_unix(datetime.datetime.min)

    # If the requested dataset is from a tree not known to the calling
    # instance, send them that whole tree.
    ds = json.loads(await redis.execute("hget", "datasets", ds_id))
    root = await find_root(ds_id, ds)
    if root is None:
        logger.error("update-datasets: Root of dataset {} not found.".format(ds_id))
        reply["result"] = "Root of dataset {} not found.".format(ds_id)
    if root not in roots:
        reply["datasets"] = await tree(root)

    # add a timestamp to the result before gathering update
    reply["ts"] = caput_time.datetime_to_unix(datetime.datetime.utcnow())
    reply["datasets"].update(await gather_update(ts, roots))

    reply["result"] = "success"
    logger.debug("update-datasets: Answering with {}.".format(reply))
    return response.json(reply)


async def tree(root):
    """Return a list of all nodes in the given tree."""
    async with lock_datasets as r:
        datasets_of_root = json.loads(await r.execute("hget", "datasets_of_root", root))

        # Request all datasets concurrently
        dsets = await asyncio.gather(
            *[r.execute("hget", "datasets", n) for n in datasets_of_root]
        )
        tree = dict(zip(datasets_of_root, [json.loads(ds) for ds in dsets]))
    return tree


class Broker:
    """Main class to run the comet dataset broker."""

    def __init__(self, file_lock_time, debug, recover, workers, port):

        self.config = {
            "file_lock_time": file_lock_time,
            "debug": debug,
            "recover": recover,
            "port": port,
        }

        self.debug = debug
        self.startup_time = datetime.datetime.utcnow()
        self.n_workers = workers
        self.port = None

    def _flush_redis(self):
        """
        Flush from redis what we don't want to keep on start.

        At the moment this only deletes members of the set "requested_states".
        """
        r = redis_sync.Redis(REDIS_SERVER[0], REDIS_SERVER[1])
        hash = r.spop("requested_states")
        while hash:
            logger.warning(
                "Found requested state in redis on startup: {}\nFlushing...".format(
                    hash.decode()
                )
            )
            hash = r.spop("requested_states")

    def _wait_and_register(self):

        # Wait until the port has been set (meaning comet is available)
        while not self.port:
            time.sleep(1)

        manager = Manager("localhost", self.port)
        try:
            manager.register_start(self.startup_time, __version__)
            manager.register_config(self.config)
        except CometError as exc:
            logger.error(
                "Comet failed registering its own startup and initial config: {}".format(
                    exc
                )
            )
            exit(1)

    def run_comet(self):
        """Run comet dataset broker."""

        print(
            "Starting CoMeT dataset_broker {} using port {}.".format(
                __version__, self.config["port"]
            )
        )

        self._flush_redis()

        # # Register config with broker
        t = Thread(target=self._wait_and_register)
        t.start()

        # Check if port is set to 0 for random open port
        port = self.config["port"]
        server_kwargs = {}
        if port == 0:
            sock = socket()
            sock.bind(("0.0.0.0", 0))
            server_kwargs["sock"] = sock
            _, port = sock.getsockname()
            logger.info("Selected random port: {}".format(port))
        else:
            server_kwargs["host"] = "0.0.0.0"
            server_kwargs["port"] = port
        self.port = port

        app.run(
            workers=self.n_workers,
            return_asyncio_server=True,
            access_log=self.debug,
            debug=self.debug,
            **server_kwargs
        )

        t.join()
        logger.info("Comet stopped.")


async def create_locks():
    """Create all redis locks."""
    global lock_states, lock_datasets, lock_external_states, cond_states, cond_datasets

    lock_states = await Lock.create(redis, "states")
    lock_datasets = await Lock.create(redis, "datasets")
    lock_external_states = await Lock.create(redis, "external_datasets")
    cond_states = await Condition.create(lock_states, "states")
    cond_datasets = await Condition.create(lock_datasets, "datasets")


async def close_locks():
    """Create all redis locks."""
    # Free the condition variables first as this requires the lock to do so.
    # Ignore if lock already closed by another worker.
    try:
        await cond_states.close()
    except LockError:
        pass
    try:
        await cond_datasets.close()
    except LockError:
        pass
    try:
        await lock_states.close()
    except LockError:
        pass
    try:
        await lock_datasets.close()
    except LockError:
        pass
    try:
        await lock_external_states.close()
    except LockError:
        return


# Create the Redis connection pool, use sanic to start it so that it
# ends up in the same event loop
# At the same time create the locks that we will need
async def _init_redis_async(_, loop):
    global redis
    redis = await aioredis.create_pool(
        REDIS_SERVER, encoding="utf-8", minsize=20, maxsize=10000
    )
    await create_locks()


async def _close_redis_async(_, loop):
    await close_locks()
    redis.close()
    await redis.wait_closed()


app.register_listener(_init_redis_async, "before_server_start")
app.register_listener(_close_redis_async, "after_server_stop")
