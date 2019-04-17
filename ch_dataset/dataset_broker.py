#!/usr/bin/env python
"""
REST Server and clients for the Dataset Broker.

"""
# Python Standard Library packages
import logging
import sys
import thread
import datetime
from bisect import bisect_left

# PyPi packages
import toro  # conditional variables for tornado coroutines

# External private packages
from wtl.rest import AsyncRESTClient, AsyncRESTServer, endpoint
from wtl.rest import coroutine, coroutine_return
from wtl.rest import run_client  # generic REST servers and clients

# Local imports

from version import __version__

# This is how a log line will look like:
LOG_FORMAT = '[%(asctime)s] %(name)s: %(message)s'
LOG_LEVEL = logging.DEBUG
WAIT_TIME = 40


def datetime_to_float(d):
    epoch = datetime.datetime.utcfromtimestamp(0)
    total_seconds = (d - epoch).total_seconds()
    # total_seconds will be in decimals (millisecond precision)
    return total_seconds


def float_to_datetime(fl):
    return datetime.datetime.utcfromtimestamp(fl)

class DSBrokerAsyncRESTServer(AsyncRESTServer):
    """
    REST interface for Dataset Broker.
    """

    DEFAULT_PORT = 12050

    def __init__(self, address='', port=DEFAULT_PORT, logging_params={}):
        """
        List of dict with entries 'type', 'name', and 'address'
        """
        self.log = logging.getLogger("dataset_broker")
        self.states = dict()

        # Hashmap of datasets for direct access.
        self.datasets = dict()

        # Two dicts of root -> list of (ds_id/timestamp) / timesamp. The latter
        # facilitates sorted insert into the first.
        self.datasets_of_root = dict()
        self.datasets_of_root_keys = dict()

        self.signal_states_updated = toro.Condition()
        self.signal_datasets_updated = toro.Condition()
        self.lock_datasets = thread.allocate_lock()
        self.lock_states = thread.allocate_lock()

        # a set of requested states and its lock
        self.requested_states = set()
        self.lock_requested_states = thread.allocate_lock()

        super(DSBrokerAsyncRESTServer, self).__init__(address=address,
                                                      port=port,
                                                      heartbeat_string='Gs')

    ##################
    # Server commands
    ##################

    @coroutine
    @endpoint('status')
    def status(self, handler):
        """ Get status of dataset-broker.

        Shows all datasets and states registered by the broker.

        curl
        -X GET
        http://localhost:12050/status
        """
        self.log.debug('status: Received status request')
        reply = dict()
        with self.lock_states:
            reply["states"] = self.states.keys()
        with self.lock_datasets:
            reply["datasets"] = self.datasets.keys()
        coroutine_return(reply)
        self.log.debug('status: states: %r' % (self.states.keys()))
        self.log.debug('status: datasets: %r' % (self.datasets.keys()))

    @coroutine
    @endpoint('register-state')
    def registerState(self, handler, hash):
        """ Register a dataset state with the broker.

        This should only ever be called by kotekan's datasetManager.
        """
        self.log.debug('register-state: Receiving register state request, hash: %r' % (hash))
        reply = dict(result="success")
        with self.lock_states:
            if self.states.get(hash) is None:
                # we don't know this state, did we request it already?
                with self.lock_requested_states:
                    if hash in self.requested_states:
                        coroutine_return(reply)

                # ask for it
                with self.lock_requested_states:
                    self.requested_states.add(hash)
                reply['request'] = "get_state"
                reply['hash'] = hash
                self.log.debug('register-state: Asking for state, hash: %r'
                               % (hash))
        self.log.debug('register-state: Received register state request, hash: %r'
                       % (hash))
        coroutine_return(reply)

    @coroutine
    @endpoint('send-state')
    def sendState(self, handler, hash, state):
        """ Send a dataset state to the broker.

        This should only ever be called by kotekan's datasetManager.
        """
        self.log.debug('send-state: Received state %r' % (hash))
        reply = dict()

        # do we have this state already?
        with self.lock_states:
            found = self.states.get(hash)
            if found is not None:
                # if we know it already, does it differ?
                if found != state:
                    reply['result'] = "error: a different state is know to " \
                                      "the broker with this hash: %r" % found
                    self.log.warn('send-state: Failure receiving state: a '
                                  'different state with the same hash is: %r'
                                  % (found))
                else:
                    reply['result'] = "success"
            else:
                self.states[hash] = state
                reply['result'] = "success"
                self.signal_states_updated.notify_all()

        with self.lock_requested_states:
            self.requested_states.remove(hash)

        coroutine_return(reply)

    @coroutine
    @endpoint('register-dataset')
    def registerDataset(self, handler, hash, ds):
        """ Register a dataset with the broker.

        This should only ever be called by kotekan's datasetManager.
        """
        self.log.debug('register-dataset: Registering new dataset with hash %r : %r' %
                       (hash, ds))
        dataset_valid = yield self.checkDataset(ds)
        reply = dict()
        root = yield self.findRoot(hash, ds)

        # dataset already known?
        with self.lock_datasets:
            found = self.datasets.get(hash)
            if found is not None:
                # if we know it already, does it differ?
                if found != ds:
                    reply['result'] = "error: a different dataset is know to" \
                                    " the broker with this hash: %r" % found
                    self.log.warn('register-dataset: Failure receiving dataset: a'
                                  ' different dataset with the same hash is: %r'
                                  % (found))
                else:
                    reply['result'] = "success"
            elif dataset_valid:
                # save the dataset
                yield self.saveDataset(hash, ds, root)

                reply['result'] = "success"
                self.signal_datasets_updated.notify_all()
            else:
                reply['result'] = "dataset invalid."
                self.log.debug(
                    'register-dataset: Received invalid dataset with hash %r : %r' %
                    (hash, ds))
            self.log.debug('register-dataset: Registered new dataset with hash %r : %r' %
                           (hash, ds))
            coroutine_return(reply)

    @coroutine
    def saveDataset(self, hash, ds, root):
        """ Save the given dataset, its hash and a current timestamp.

            This should be called while a lock on the datasets is held.
        """

        # add a timestamp to the dataset (ms precision)
        ts = datetime_to_float(datetime.datetime.utcnow())

        # create entry if this is the first node with that root
        if root not in self.datasets_of_root.keys():
            self.datasets_of_root[root] = list()
            self.datasets_of_root_keys[root] = list()

        # Determine where to insert dataset ID.
        i = bisect_left(self.datasets_of_root_keys[root], ts)

        # Insert timestamp in keys list.
        self.datasets_of_root_keys[root].insert(i, ts)

        # Insert the dataset ID itself in the corresponding place.
        self.datasets_of_root[root].insert(i, hash)

        # Insert the dataset in the hashmap
        self.datasets[hash] = ds

    def gatherUpdate(self, ts, roots):
        """ Returns a dict of dataset ID -> dataset with all datasets with the
            given roots that were registered after the given timestamp.
        """
        update = dict()

        with self.lock_datasets:
            for r in roots:
                tree = reversed(self.datasets_of_root[r])
                keys = reversed(self.datasets_of_root_keys[r])

                # The nodes in tree are ordered by their timestamp from new to
                # old, so we are done as soon as we find an older timestamp than
                # the given one.
                for n,k in zip(tree,keys):
                    if k < ts:
                        break
                    update[n] = self.datasets[n]
        return update

    @coroutine
    def findRoot(self, hash, ds):
        """ Returns the dataset Id of the root of this dataset.
        """
        root = hash
        while not ds['is_root']:
            root = ds['base_dset']
            found = yield self.wait_for_dset(root)
            if not found:
                self.log.error('findRoot: dataset %r not found.', hash)
                coroutine_return(None)
            with self.lock_datasets:
                ds = self.datasets[root]
        coroutine_return(root)

    @coroutine
    def checkDataset(self, ds):
        """ Checks if a dataset is valid.

        For a dataset to be valid, the state and base dataset it references to
        have to exist. If it is a root dataset, the base dataset does not have
        to exist.
        """
        self.log.debug('checkDataset: Checking dataset: %r' %
                       (ds))
        found = yield self.wait_for_state(ds['state'])
        if not found:
            self.log.debug('checkDataset: State of dataset unknown: %r' %
                           (ds))
            coroutine_return(False)
        if ds['is_root']:
            self.log.debug('checkDataset: Checked dataset: %r' %
                           (ds))
            coroutine_return(True)
        found = yield self.wait_for_dset(ds['base_dset'])
        if not found:
            self.log.debug('checkDataset: Base dataset of dataset unknown: %r' %
                           (ds))
            coroutine_return(False)
        self.log.debug('checkDataset: Checked dataset: %r' %
                       (ds))
        coroutine_return(True)

    @coroutine
    @endpoint('request-state')
    def requestState(self, handler, id):
        """ Request the state with the given ID.

        This is called by kotekan's datasetManager.

        curl
        -d '{"state_id":42}'
        -X POST
        -H "Content-Type: application/json"
        http://localhost:12050/request-state
        """
        self.log.debug('request-state: Received request for state with ID %r'
                       % (id))
        reply = dict()
        reply['id'] = id

        # Do we know this state ID?
        self.log.debug(
            'request-state: waiting for state ID %r' % (id))
        found = yield self.wait_for_state(id)
        if not found:
            reply['result'] = "state ID %r unknown to broker." % id
            self.log.info('request-state: State %r unknown to broker' % (id))
            coroutine_return(reply)
        self.log.debug(
            'request-state: found state ID %r' % (id))

        with self.lock_states:
            reply['state'] = self.states[id]

        reply['result'] = "success"
        self.log.debug(
            'request-state: Replying with state %r' % (id))
        coroutine_return(reply)

    @coroutine
    def wait_for_dset(self, id):
        found = True
        self.lock_datasets.acquire()

        if self.datasets.get(id) is None:
            # wait for half of kotekans timeout before we admit we don't have it
            self.lock_datasets.release()
            notified = True
            self.log.debug('wait_for_ds: Waiting for dataset %r' % (id))
            try:
                while True:
                    notified = yield self.signal_datasets_updated.wait(
                        deadline=datetime.timedelta(seconds=WAIT_TIME))
                    # did someone send it to us by now?
                    with self.lock_datasets:
                        if self.datasets.get(id) is not None:
                            self.log.debug(
                                'wait_for_ds: Found dataset %r' % (id))
                            break
            except toro.Timeout as e:
                pass
            self.lock_datasets.acquire()
            if self.datasets.get(id) is None:
                self.log.warn('wait_for_ds: Timeout (%rs) when waiting for dataset %r'
                              % (WAIT_TIME, id))
                found = False
        self.lock_datasets.release()

        coroutine_return(found)

    @coroutine
    def wait_for_state(self, id):
        found = True
        self.lock_states.acquire()
        if self.states.get(id) is None:
            # wait for half of kotekans timeout before we admit we don't have it
            self.lock_states.release()
            notified = True
            self.log.debug('wait_for_state: Waiting for state %r' % (id))
            try:
                while True:
                    notified = yield self.signal_states_updated.wait(
                        deadline=datetime.timedelta(seconds=WAIT_TIME))
                    # did someone send it to us by now?
                    with self.lock_states:
                        if self.states.get(id) is not None:
                            self.log.debug(
                                'wait_for_state: Got state %r' % (id))
                            break
            except toro.Timeout as e:
                pass
            self.lock_states.acquire()
            if self.states.get(id) is None:
                self.log.warn('wait_for_state: Timeout (%rs) when waiting for state %r'
                              % (WAIT_TIME, id))
                found = False
        self.lock_states.release()

        coroutine_return(found)

    @coroutine
    @endpoint('update-datasets')
    def updateDatasets(self, handler, ds_id, ts, roots):
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
        self.log.debug('update-datasets: Received request for ancestors of dataset %r '
                       'since timestamp %r, roots %r.'
                       % (ds_id, ts, roots))
        reply = dict()
        reply['datasets'] = dict()

        # Do we know this ds ID?
        found = yield self.wait_for_dset(ds_id)
        if not found:
            reply['result'] = "update-datasets: Dataset ID %r unknown to broker." % ds_id
            self.log.info('update-datasets: Dataset ID %r unknown to broker'
                          % (ds_id))
            coroutine_return(reply)

        if ts is 0:
            ts = datetime_to_float(datetime.datetime.min)

        # If the requested dataset is from a tree not known to the calling
        # instance, send them that whole tree.
        root = yield self.findRoot(ds_id, self.datasets[ds_id])
        if root is None:
            self.log.error('update-datasets: Root of dataset %r not found.', ds_id)
            reply['result'] = 'Root of dataset %r not found.' % ds_id
        if root not in roots:
            reply['datasets'] = self.tree(root)

        # add a timestamp to the result before gathering update
        reply['ts'] = datetime_to_float(datetime.datetime.utcnow())
        reply['datasets'].update(self.gatherUpdate(ts, roots))

        reply['result'] = "success"
        self.log.debug('update-datasets: Answering with %r.' % (reply))
        coroutine_return(reply)

    def tree(self, root):
        """ Returns a list of all nodes in the given tree. """
        tree = dict()
        with self.lock_datasets:
            for n in self.datasets_of_root[root]:
                tree[n] = self.datasets[n]
        return tree



#########################################
# Dataset Broker REST client
#########################################

class DSBrokerAsyncRESTClient(AsyncRESTClient):
    """
    Implements an asynchronous client that exposes the functions of the
    specified remote dataset broker.

    The client is implemented using a Tornado AsyncHTTPClient. It exposes the
    dataset broker methods
    (i.e REST endpoints) as local methods. The local methods are Tornado
    coroutines so requests to
    multiple clients can be made in parallel. This is especially beneficial
    since the data requests
    from the server are slow IO operations which benefit the mist from
    co-execution.

    The client will operate only if the IOloop in which is was created is
    running.

    Parameters:

        name (str): Name of the client, to be used in logging etc.

        hostname (str): The hostname of the dataset broker. If `host` is None,
        an (experimental,
             Python-based) dataset broker REST server will be created locally.

        port (int): The port number to which the dataset broker REST server is
        listening. Default is port 80.
    """
    DEFAULT_PORT = DSBrokerAsyncRESTServer.DEFAULT_PORT

    def __init__(self, hostname='localhost', port=DEFAULT_PORT):
        super(DSBrokerAsyncRESTClient, self).__init__(
            hostname=hostname, port=port,
            server_class=DSBrokerAsyncRESTServer,
            heartbeat_string='Gc')

    @coroutine
    def registerState(self, hash):
        result = yield self.post('register-state', hash)
        coroutine_return(result)

    @coroutine
    def sendState(self, hash, state):
        result = yield self.post('send-state', hash, state)
        coroutine_return(result)

    @coroutine
    def registerDataset(self, hash, ds):
        result = yield self.post('register-dataset', hash, ds)
        coroutine_return(result)

    @coroutine
    def status(self):
        result = yield self.get('status')
        coroutine_return(result)

    @coroutine
    def requestState(self, state_id):
        result = yield self.post('request-state', id)
        coroutine_return(result)

    @coroutine
    def updateDatasets(self, ds_id, ts):
        result = yield self.post('update-datasets', ds_id, ts)


def main():
    """ Command-line interface to launch and operate the dataset broker.
    """
    client, server = run_client(sys.argv[1:],
                                DSBrokerAsyncRESTServer,
                                DSBrokerAsyncRESTClient,
                                object_name='DSETBROKER',
                                server_config_path='dsetbroker.servers')
    return client, server


if __name__ == '__main__':
    client, server = main()
