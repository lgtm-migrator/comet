#!/usr/bin/env python
"""
REST Server and clients for CoMeT (the Dataset Broker).

"""
import logging
import time
import datetime
from bisect import bisect_left
import signal
import sys
from threading import Thread, Lock, Condition

from flask import Flask, jsonify, request
from werkzeug.serving import make_server

from . import __version__

# This is how a log line will look like:
LOG_FORMAT = '[%(asctime)s] %(name)s: %(message)s'
LOG_LEVEL = logging.DEBUG
WAIT_TIME = 40
DEFAULT_PORT = 12050

log = logging.getLogger("CoMeT")
app = Flask(__name__)


def datetime_to_float(d):
    epoch = datetime.datetime.utcfromtimestamp(0)
    total_seconds = (d - epoch).total_seconds()
    # total_seconds will be in decimals (millisecond precision)
    return total_seconds


def float_to_datetime(fl):
    return datetime.datetime.utcfromtimestamp(fl)


# Global state variables
states = dict()

# Hashmap of datasets for direct access.
datasets = dict()

# Two dicts of root -> list of (ds_id/timestamp) / timesamp. The latter
# facilitates sorted insert into the first.
datasets_of_root = dict()
datasets_of_root_keys = dict()

lock_datasets = Lock()
lock_states = Lock()
signal_states_updated = Condition(lock_states)
signal_datasets_updated = Condition(lock_datasets)

# a set of requested states and its lock
requested_states = set()
lock_requested_states = Lock()


@app.route('/status', methods=['GET'])
def status():
    """ Get status of CoMeT (dataset-broker).

    Shows all datasets and states registered by CoMeT (the broker).

    curl -X GET http://localhost:12050/status
    """
    global states
    global datasets

    log.debug('status: Received status request')

    reply = dict()
    with lock_states:
        reply["states"] = states.keys()
    with lock_datasets:
        reply["datasets"] = datasets.keys()

    log.debug('states: %r' % (states.keys()))
    log.debug('datasets: %r' % (datasets.keys()))

    return jsonify(reply)


@app.route('/register-state', methods=['POST'])
def registerState():
    """ Register a dataset state with CoMeT (the broker).

    This should only ever be called by kotekan's datasetManager.
    """
    global states
    global requested_states

    hash = request.get_json(force=True)['hash']
    log.debug('register-state: Received register state request, hash: %r' % (hash))
    reply = dict(result="success")

    with lock_states:
        if states.get(hash) is None:
            # we don't know this state, did we request it already?
            with lock_requested_states:
                if hash in requested_states:
                    return jsonify(reply)

            # ask for it
            with lock_requested_states:
                requested_states.add(hash)
            reply['request'] = "get_state"
            reply['hash'] = hash
            log.debug('register-state: Asking for state, hash: %r' % (hash))
    return jsonify(reply)


@app.route('/send-state', methods=['POST'])
def sendState():
    """ Send a dataset state to CoMeT (the broker).

    This should only ever be called by kotekan's datasetManager.
    """
    global states
    global requested_states

    hash = request.get_json(force=True)['hash']
    state = request.get_json(force=True)['state']
    log.debug('send-state: Received state %r' % (hash))
    reply = dict()

    # do we have this state already?
    with lock_states:
        found = states.get(hash)
        if found is not None:
            # if we know it already, does it differ?
            if found != state:
                reply['result'] = "error: a different state is know to " \
                                  "the broker with this hash: %r" % found
                log.warn('send-state: Failure receiving state: a '
                         'different state with the same hash is: %r'
                         % (found))
            else:
                reply['result'] = "success"
        else:
            states[hash] = state
            reply['result'] = "success"
            signal_states_updated.notify_all()

    with lock_requested_states:
        requested_states.remove(hash)

    return jsonify(reply)


@app.route('/register-dataset', methods=['POST'])
def registerDataset():
    """ Register a dataset with CoMeT (the broker).

    This should only ever be called by kotekan's datasetManager.
    """
    global datasets

    hash = request.get_json(force=True)['hash']
    ds = request.get_json(force=True)['ds']
    log.debug('register-dataset: Registering new dataset with hash %r : %r' % (hash, ds))
    dataset_valid = checkDataset(ds)
    reply = dict()
    root = findRoot(hash, ds)

    # dataset already known?
    with lock_datasets:
        found = datasets.get(hash)
        if found is not None:
            # if we know it already, does it differ?
            if found != ds:
                reply['result'] = "error: a different dataset is know to" \
                                  " the broker with this hash: %r" % found
                log.warn('register-dataset: Failure receiving dataset: a'
                         ' different dataset with the same hash is: %r' % (found))
            else:
                reply['result'] = "success"
        elif dataset_valid:
            # save the dataset
            saveDataset(hash, ds, root)

            reply['result'] = "success"
            signal_datasets_updated.notify_all()
        else:
            reply['result'] = "dataset invalid."
            log.debug(
                'register-dataset: Received invalid dataset with hash %r : %r' %
                (hash, ds))
        return jsonify(reply)


def saveDataset(hash, ds, root):
    """ Save the given dataset, its hash and a current timestamp.

        This should be called while a lock on the datasets is held.
    """

    # add a timestamp to the dataset (ms precision)
    ts = datetime_to_float(datetime.datetime.utcnow())

    # create entry if this is the first node with that root
    if root not in datasets_of_root.keys():
        datasets_of_root[root] = list()
        datasets_of_root_keys[root] = list()

    # Determine where to insert dataset ID.
    i = bisect_left(datasets_of_root_keys[root], ts)

    # Insert timestamp in keys list.
    datasets_of_root_keys[root].insert(i, ts)

    # Insert the dataset ID itself in the corresponding place.
    datasets_of_root[root].insert(i, hash)

    # Insert the dataset in the hashmap
    datasets[hash] = ds


def gatherUpdate(ts, roots):
    """ Returns a dict of dataset ID -> dataset with all datasets with the
        given roots that were registered after the given timestamp.
    """
    update = dict()

    with lock_datasets:
        for r in roots:
            tree = reversed(datasets_of_root[r])
            keys = reversed(datasets_of_root_keys[r])

            # The nodes in tree are ordered by their timestamp from new to
            # old, so we are done as soon as we find an older timestamp than
            # the given one.
            for n, k in zip(tree, keys):
                if k < ts:
                    break
                update[n] = datasets[n]
    return update


def findRoot(hash, ds):
    """Returns the dataset Id of the root of this dataset."""
    root = hash
    while not ds['is_root']:
        root = ds['base_dset']
        found = wait_for_dset(root)
        if not found:
            log.error('findRoot: dataset %r not found.', hash)
            return None
        with lock_datasets:
            ds = datasets[root]
    return root


def checkDataset(ds):
    """Check if a dataset is valid.

    For a dataset to be valid, the state and base dataset it references to
    have to exist. If it is a root dataset, the base dataset does not have
    to exist.
    """
    log.debug('checkDataset: Checking dataset: %r' % (ds))
    found = wait_for_state(ds['state'])
    if not found:
        log.debug('checkDataset: State of dataset unknown: %r' % (ds))
        return False
    if ds['is_root']:
        log.debug('checkDataset: Checked dataset: %r' %
                  (ds))
        return True
    found = wait_for_dset(ds['base_dset'])
    if not found:
        log.debug('checkDataset: Base dataset of dataset unknown: %r' % (ds))
        return False
    return True


@app.route('/request-state', methods=['POST'])
def requestState():
    """ Request the state with the given ID.

    This is called by kotekan's datasetManager.

    curl -d '{"state_id":42}' -X POST -H "Content-Type: application/json"
         http://localhost:12050/request-state
    """
    global states
    id = request.get_json(force=True)['id']

    log.debug('request-state: Received request for state with ID %r' % (id))
    reply = dict()
    reply['id'] = id

    # Do we know this state ID?
    log.debug('request-state: waiting for state ID %r' % (id))
    found = wait_for_state(id)
    if not found:
        reply['result'] = "state ID %r unknown to broker." % id
        log.info('request-state: State %r unknown to broker' % (id))
        return jsonify(reply)
    log.debug('request-state: found state ID %r' % (id))

    with lock_states:
        reply['state'] = states[id]

    reply['result'] = "success"
    log.debug('request-state: Replying with state %r' % (id))
    return jsonify(reply)


def wait_for_dset(id):
    global datasets

    found = True
    lock_datasets.acquire()

    if datasets.get(id) is None:
        # wait for half of kotekans timeout before we admit we don't have it
        lock_datasets.release()
        log.debug('wait_for_ds: Waiting for dataset %r' % (id))
        while True:
            # did someone send it to us by now?
            with lock_datasets:
                signal_datasets_updated.wait(WAIT_TIME)
                if datasets.get(id) is not None:
                    log.debug(
                        'wait_for_ds: Found dataset %r' % (id))
                    break

        lock_datasets.acquire()
        if datasets.get(id) is None:
            log.warn('wait_for_ds: Timeout (%rs) when waiting for dataset %r' % (WAIT_TIME, id))
            found = False
    lock_datasets.release()

    return found


def wait_for_state(id):
    global states

    found = True
    lock_states.acquire()
    if states.get(id) is None:
        # wait for half of kotekans timeout before we admit we don't have it
        lock_states.release()
        log.debug('wait_for_state: Waiting for state %r' % (id))
        while True:
            # did someone send it to us by now?
            with lock_states:
                signal_states_updated.wait(WAIT_TIME)
                if states.get(id) is not None:
                    log.debug(
                        'wait_for_state: Got state %r' % (id))
                    break

        lock_states.acquire()
        if states.get(id) is None:
            log.warn('wait_for_state: Timeout (%rs) when waiting for state %r' % (WAIT_TIME, id))
            found = False
    lock_states.release()

    return found


@app.route('/update-datasets', methods=['POST'])
def updateDatasets():
    """
    Request all nodes that where added after the given timestamp.
    If the root of the given dataset is not among the given known roots,
    All datasets with the same root as the given dataset are included in the
    returned update additionally.

    This is called by kotekan's datasetManager.

    curl
    -d '{"ds_id":2143,"ts":0}'
    -X POST
    -H "Content-Type: application/json"
    http://localhost:12050/update-datasets
    """
    global datasets
    ds_id = request.get_json(force=True)['ds_id']
    ts = request.get_json(force=True)['ts']
    roots = request.get_json(force=True)['roots']

    log.debug('update-datasets: Received request for ancestors of dataset %r '
              'since timestamp %r, roots %r.' % (ds_id, ts, roots))
    reply = dict()
    reply['datasets'] = dict()

    # Do we know this ds ID?
    found = wait_for_dset(ds_id)
    if not found:
        reply['result'] = "update-datasets: Dataset ID %r unknown to broker." % ds_id
        log.info('update-datasets: Dataset ID %r unknown.' % (ds_id))
        return jsonify(reply)

    if ts is 0:
        ts = datetime_to_float(datetime.datetime.min)

    # If the requested dataset is from a tree not known to the calling
    # instance, send them that whole tree.
    root = findRoot(ds_id, datasets[ds_id])
    if root is None:
        log.error('update-datasets: Root of dataset %r not found.', ds_id)
        reply['result'] = 'Root of dataset %r not found.' % ds_id
    if root not in roots:
        reply['datasets'] = tree(root)

    # add a timestamp to the result before gathering update
    reply['ts'] = datetime_to_float(datetime.datetime.utcnow())
    reply['datasets'].update(gatherUpdate(ts, roots))

    reply['result'] = "success"
    log.debug('update-datasets: Answering with %r.' % (reply))
    return jsonify(reply)


def tree(root):
    """ Returns a list of all nodes in the given tree. """
    tree = dict()
    with lock_datasets:
        for n in datasets_of_root[root]:
            tree[n] = datasets[n]
    return tree


class ServerThread(Thread):
    """
    REST interface for Dataset Broker.
    """

    def __init__(self, app, address='0.0.0.0', port=DEFAULT_PORT):
        """
        List of dict with entries 'type', 'name', and 'address'
        """
        log.info("Starting CoMeT dataset_broker({}) using port {}.".format(__version__, port))

        Thread.__init__(self)
        self.srv = make_server(address, port, app, threaded=True)
        self.ctx = app.app_context()
        self.ctx.push()

    def run(self):
        self.srv.serve_forever()

    def shutdown(self):
        self.srv.shutdown()


class Comet():
    def __init__(self):
        signal.signal(signal.SIGINT, self.signal_handler)
        logging.basicConfig(format=LOG_FORMAT)
        log.setLevel(LOG_LEVEL)
        self.server = ServerThread(app)

    def run(self):
        self.server.start()
        while True:
            time.sleep(5)

    def signal_handler(self, sig, frame):
        self.server.shutdown()
        sys.exit(0)
