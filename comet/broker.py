"""REST Server for CoMeT (the Dataset Broker)."""
import aioredis
import asyncio
import datetime
import json
import os

from bisect import bisect_left
from caput import time
from signal import signal, SIGINT
from threading import Thread
from time import sleep

from sanic import Sanic
from sanic import response
from sanic.log import logger
from concurrent.futures import CancelledError

from . import __version__
from .dumper import Dumper
from .manager import Manager, CometError, TIMESTAMP_FORMAT
from .redis_async_locks import (
    redis_condition_notify,
    redis_condition_wait,
    Lock,
    redis_lock_create,
    redis_lock_acquire,
    redis_lock_release,
)

WAIT_TIME = 40
DEFAULT_PORT = 12050
redis_instance = [("localhost", 6379)]

app = Sanic(__name__)
app.config.REQUEST_TIMEOUT = 600
app.config.RESPONSE_TIMEOUT = 600


@app.route("/status", methods=["GET"])
async def status(request):
    """
    Get status of CoMeT (dataset-broker).

    Shows all datasets and states registered by CoMeT (the broker).

    curl -X GET http://localhost:12050/status
    """
    logger.debug("status: Received status request")

    reply = dict()

    # Get all states and datasets from redis concurrently
    get_states_task = asyncio.ensure_future(redis.execute("hkeys", "states"))
    reply["datasets"] = await redis.execute("hkeys", "datasets")
    (get_states_task,), _ = await asyncio.wait({get_states_task})
    reply["states"] = get_states_task.result()

    logger.debug("states: {}".format(reply["states"]))
    logger.debug("datasets: {}".format(reply["datasets"]))

    return response.json(reply)


@app.route("/register-external-state", methods=["POST"])
async def external_state(request):
    """Register an external state that is detached from any dataset."""
    hash = request.json["hash"]
    type = request.json["type"]
    logger.debug("Received external state: {} with hash {}".format(type, hash))

    result = await register_state(request)

    async with Lock("external_states"):
        await redis.execute("hset", "external_state", type, hash)
        if request.json.get("dump", True):
            ext_state_list = await redis.execute("hgetall", "external_state")
            # hgetall returns a list with keys and values... sort it into a dict:
            ext_state_dict = dict(zip(ext_state_list[0::2], ext_state_list[1::2]))
            ext_state_dump = {"external-state": ext_state_dict}

    # Dump to file:
    if request.json.get("dump", True):
        if "time" in request.json:
            ext_state_dump["time"] = request.json["time"]
        await dumper.dump(ext_state_dump, redis)

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
    async with Lock(redis, "states"):
        state = await redis.execute("hget", "states", hash)
        if state is None:
            # we don't know this state, did we request it already?
            if await redis.execute("sismember", "requested_states", hash):
                return response.json(reply)

            # otherwise, request it now
            await redis.execute("sadd", "requested_states", hash)
            reply["request"] = "get_state"
            reply["hash"] = hash
            logger.debug("register-state: Asking for state, hash: {}".format(hash))

    if request.json.get("dump", True):
        if state is not None:
            # Dump state to file
            state_dump = {"state": None, "hash": hash}
            await dumper.dump(state_dump, redis)

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
    async with Lock(redis, "states"):
        found = await redis.execute("hget", "states", hash)
        if found is not None:
            # if we know it already, does it differ?
            if found != state:
                reply["result"] = (
                    "error: hash collision ({})\nTrying to register the following dataset state:\n{},\nbut a different state is know to "
                    "the broker with the same hash:\n{}".format(hash, state, found)
                )
                logger.warning(
                    "send-state: {}".format(reply["result"])
                )
            else:
                reply["result"] = "success"
        else:
            await redis.execute("hset", "states", hash, json.dumps(state))
            reply["result"] = "success"
            await redis_condition_notify(redis, "states")

    # Remove it from the set of requested states (if it's in there.)
    await redis.execute("srem", "requested_states", hash)

    if request.json.get("dump", True):
        # Dump state to file
        state_dump = {"state": state, "hash": hash}
        await dumper.dump(state_dump, redis)

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

    # Only dump new datasets.
    dump = False

    # Lack datasets and check if dataset already known.
    async with Lock(redis, "datasets"):
        found = await redis.execute("hget", "datasets", hash)
        if found is not None:
            # this string needs to be deserialized, contains a dataset
            found = json.loads(found)
            # if we know it already, does it differ?
            if found != ds:
                reply["result"] = (
                    "error: hash collision ({})\nTrying to register the following dataset:\n{},\nbut a different one is know to "
                    "the broker with the same hash:\n{}".format(hash, ds, found)
                )
                logger.warning(
                    "send-state: {}".format(reply["result"])
                )
            else:
                reply["result"] = "success"
        elif dataset_valid:
            # save the dataset
            await save_dataset(hash, ds, root)
            dump = request.json.get("dump", True)

            reply["result"] = "success"
            await redis_condition_notify(redis, "datasets")
        else:
            reply["result"] = "Dataset {} invalid.".format(hash)
            logger.debug(
                "register-dataset: Received invalid dataset with hash {} : {}".format(
                    hash, ds
                )
            )

    # Dump dataset to file
    if dump:
        ds_dump = {"ds": ds, "hash": hash}
        if "time" in request.json:
            ds_dump["time"] = request.json["time"]
        await dumper.dump(ds_dump, redis)

    return response.json(reply)


async def save_dataset(hash, ds, root):
    """Save the given dataset, its hash and a current timestamp.

    This should be called while a lock on the datasets is held.
    """
    # add a timestamp to the dataset (ms precision)
    ts = time.datetime_to_unix(datetime.datetime.utcnow())

    # get dicts from redis concurrently
    task = asyncio.ensure_future(redis.execute("hget", "datasets_of_root", root))
    datasets_of_root_keys = await redis.execute("hget", "datasets_of_root_keys", root)
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
        redis.execute("hset", "datasets_of_root", root, json.dumps(datasets_of_root))
    )
    task2 = asyncio.ensure_future(
        redis.execute(
            "hset", "datasets_of_root_keys", root, json.dumps(datasets_of_root_keys)
        )
    )

    # Insert the dataset in the hashmap
    task3 = asyncio.ensure_future(
        redis.execute("hset", "datasets", hash, json.dumps(ds))
    )

    # Wait for all concurrent tasks
    await asyncio.wait({task1, task2, task3})


async def gather_update(ts, roots):
    """Gather the update for a given time and roots.

    Returns a dict of dataset ID -> dataset with all datasets with the
    given roots that were registered after the given timestamp.
    """
    async with Lock(redis, "datasets"):
        for r in roots:
            # Get both dicts from redis concurrently:
            keys_task = asyncio.ensure_future(
                redis.execute("hget", "datasets_of_root_keys", r)
            )
            tree = reversed(
                json.loads(await redis.execute("hget", "datasets_of_root", r))
            )
            (keys_task,), _ = asyncio.wait(keys_task)
            keys = reversed(json.loads(keys_task.result()))

            # The nodes in tree are ordered by their timestamp from new to
            # old, so we are done as soon as we find an older timestamp than
            # the given one.
            tasks = []
            for n, k in zip(tree, keys):
                if k < ts:
                    break
                tasks.append(
                    asyncio.ensure_future(redis.execute("hget", "datasets", n))
                )

        # Wait for all concurrent tasks
        tasks, _ = await asyncio.wait(tasks)
        # put back together the root ds IDs and the datasets
        update = dict(zip(tree, [task.result() for task in tasks]))
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
        ds = await redis.execute("hget", "datasets", root)
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

    reply["state"] = await redis.execute("hget", "states", id)

    reply["result"] = "success"
    logger.debug("request-state: Replying with state {}".format(id))
    return response.json(reply)


async def wait_for_dset(id):
    """Wait until the given dataset is present."""
    found = True
    await redis_lock_acquire(redis, "datasets")

    if not await redis.execute("hexists", "datasets", id):
        # wait for half of kotekans timeout before we admit we don't have it
        await redis_lock_release(redis, "datasets")
        logger.debug("wait_for_ds: Waiting for dataset {}".format(id))
        while True:
            # did someone send it to us by now?
            async with Lock(redis, "datasets"):
                try:
                    await asyncio.wait_for(
                        redis_condition_wait(redis, "datasets"), WAIT_TIME
                    )
                except (TimeoutError, CancelledError):
                    logger.warning(
                        "wait_for_ds: Timeout ({}s) when waiting for dataset {}".format(
                            WAIT_TIME, id
                        )
                    )
                    return False
                if await redis.execute("hexists", "datasets", id):
                    logger.debug("wait_for_ds: Found dataset {}".format(id))
                    break
        if not await redis.execute("hexists", "datasets", id):
            logger.warning(
                "wait_for_ds: Timeout ({}s) when waiting for dataset {}".format(
                    WAIT_TIME, id
                )
            )
            found = False
    else:
        await redis_lock_release(redis, "datasets")
    return found


async def wait_for_state(id):
    """Wait until the given state is present."""
    found = True
    await redis_lock_acquire(redis, "states")
    if not await redis.execute("hexists", "states", id):
        # wait for half of kotekans timeout before we admit we don't have it
        await redis_lock_release(redis, "states")
        logger.debug("wait_for_state: Waiting for state {}".format(id))
        while True:
            # did someone send it to us by now?
            async with Lock(redis, "states"):
                try:
                    await asyncio.wait_for(
                        redis_condition_wait(redis, "states"), WAIT_TIME
                    )
                except (TimeoutError, CancelledError):
                    logger.warning(
                        "wait_for_ds: Timeout ({}s) when waiting for state {}".format(
                            WAIT_TIME, id
                        )
                    )
                    return False
                if await redis.execute("hexists", "states", id):
                    logger.debug("wait_for_ds: Found state {}".format(id))
                    break
        if not await redis.execute("hexists", "states", id):
            logger.warning(
                "wait_for_state: Timeout ({}s) when waiting for state {}".format(
                    WAIT_TIME, id
                )
            )
            found = False
    else:
        await redis_lock_release(redis, "states")
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
        ts = time.datetime_to_unix(datetime.datetime.min)

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
    reply["ts"] = time.datetime_to_unix(datetime.datetime.utcnow())
    reply["datasets"].update(await gather_update(ts, roots))

    reply["result"] = "success"
    logger.debug("update-datasets: Answering with {}.".format(reply))
    return response.json(reply)


async def tree(root):
    """Return a list of all nodes in the given tree."""
    async with Lock(redis, "datasets"):
        datasets_of_root = json.loads(
            await redis.execute("hget", "datasets_of_root", root)
        )
        tasks = asyncio.ensure_future(
            redis.execute("hget", "datasets", n) for n in datasets_of_root
        )

        # Wait for all concurrent tasks
        tasks, _ = await asyncio.wait(tasks)
        tree = dict(zip(datasets_of_root, [task.result() for task in tasks]))
    return tree


class Broker:
    """Main class to run the comet dataset broker."""

    def __init__(self, data_dump_path, file_lock_time, debug, recover, workers):
        global dumper

        self.config = {
            "data_dump_path": data_dump_path,
            "file_lock_time": file_lock_time,
            "debug": debug,
            "recover": recover,
        }

        dumper = Dumper(data_dump_path, file_lock_time)
        self.debug = debug
        self.startup_time = datetime.datetime.utcnow()
        self.n_workers = workers

    @staticmethod
    def _wait_and_register(startup_time, config):
        global dumper
        sleep(1)
        manager = Manager("localhost", DEFAULT_PORT)
        try:
            manager.register_start(startup_time, __version__)
        except CometError as exc:
            logger.error(
                "Comet failed registering its own startup and initial config: {}".format(
                    exc
                )
            )
            del dumper
            exit(1)

        if config["recover"]:
            logger.info("Reading dump files to recover state.")
            # Find the dump files
            dump_files = os.listdir(config["data_dump_path"])
            dump_files = list(filter(lambda x: x.endswith("data.dump"), dump_files))
            dump_times = [f[:-10] for f in dump_files]
            dump_times = [
                datetime.datetime.strptime(t, TIMESTAMP_FORMAT) for t in dump_times
            ]
            if dump_files:
                dump_times, dump_files = zip(*sorted(zip(dump_times, dump_files)))

            threads = list()
            for dfile in dump_files:
                logger.info("Reading dump file: {}".format(dfile))
                with open(
                    os.path.join(config["data_dump_path"], dfile), "r"
                ) as json_file:
                    line_num = 0
                    for line in json_file:
                        line_num += 1
                        entry = json.loads(line)
                        if "state" in entry.keys():
                            # Don't register the start state we just sent.
                            state = entry["state"]
                            if not state == manager.states[manager.start_state]:
                                if state:
                                    state_type = state["type"]
                                else:
                                    state_type = None
                                manager.register_state(
                                    entry["state"],
                                    state_type,
                                    False,
                                    entry["time"],
                                    entry["hash"],
                                )
                        elif "ds" in entry.keys():
                            # States need to be registered parallelly, because some registrations
                            # make the broker wait for another state.
                            threads.append(
                                Thread(
                                    target=manager.register_dataset,
                                    args=(
                                        entry["ds"]["state"],
                                        entry["ds"].get("base_dset", None),
                                        entry["ds"]["types"],
                                        entry["ds"]["is_root"],
                                        False,
                                        entry["time"],
                                        entry["hash"],
                                    ),
                                )
                            )
                            threads[-1].start()
                        else:
                            logger.warn(
                                "Dump file entry {}:{} has neither state nor dataset. "
                                "Skipping...\nThis is the entry: {}".format(
                                    dfile, line_num, entry
                                )
                            )

            for t in threads:
                t.join()

        manager.register_config(config)

    def run_comet(self):
        """Run comet dataset broker."""
        global dumper

        print(
            "Starting CoMeT dataset_broker({}) using port {}.".format(
                __version__, DEFAULT_PORT
            )
        )

        # Create all redis locks before doing anything
        asyncio.run(create_locks())

        # Register config with broker
        t = Thread(
            target=self._wait_and_register, args=(self.startup_time, self.config)
        )
        t.start()

        app.run(
            workers=self.n_workers,
            host="0.0.0.0",
            port=DEFAULT_PORT,
            return_asyncio_server=True,
            access_log=True,
            debug=self.debug,
        )


async def create_locks():
    """Create all redis locks."""
    init_redis = await aioredis.create_redis(("127.0.0.1", 6379), encoding="utf-8")
    await redis_lock_create(init_redis, "states")
    await redis_lock_create(init_redis, "datasets")
    await redis_lock_create(init_redis, "external_states")
    await redis_lock_create(init_redis, "dump")
    init_redis.close()
    await init_redis.wait_closed()


# Create the Redis connection pool, use sanic to start it so that it
# ends up in the same event loop
async def _init_redis_async(_, loop):
    global redis
    redis = await aioredis.create_redis_pool(("127.0.0.1", 6379), encoding="utf-8")


async def _close_redis_async(_, loop):
    redis.close()
    await redis.wait_closed()


app.register_listener(_init_redis_async, "before_server_start")
app.register_listener(_close_redis_async, "after_server_stop")
