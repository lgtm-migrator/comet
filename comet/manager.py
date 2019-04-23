"""CoMeT dataset manager."""

import logging
import requests
import json

# Endpoint names:
REGISTER_STATE = '/register-state'
REGISTER_DATASET = '/register-dataset'
SEND_STATE = '/send-state'


class ManagerError(BaseException):
    """There was an internal error in dataset management."""

    pass


class BrokerError(BaseException):
    """There was an error registering states or datasets with the broker."""

    pass


class Manager:
    """
    CoMeT dataset manager.

    An interface for the CoMeT dataset broker. State changes are passed to the manager,
    it takes care of registering everything with the broker and keeps local copies to reduce
    network traffic.

    Note: Only supports one-dimensional dataset state topologies. That means all states are always
    stacked on top of each other, disregarding the type/name. If you want two separate stacks,
    create two instances of this class.
    """

    def __init__(self, host, port, noconfig=False):
        """Set up the CoMeT dataset manager.

        Parameters
        ----------
        host : str
            Dataset broker host.
        port : int
            Dataset broker port.
        noconfig : bool
            If this is `True`, the manager doesn't check if the config was registered already, when
            a state is registered. Only use this if you know what you are doing. Default: `False`.
        """
        self.broker = 'http://{}:{}'.format(host, port)
        self.noconfig = noconfig
        self.config_registered = False
        self.states = dict()
        self.datasets = list()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

    def register_config(self, config):
        """Register a static config with the broker.

        This should only be called once. If you want to register a state that may change, use
        :function:`register_state` instead.

        Parameters
        ----------
        config : dict
            The config should be JSON-serializable, preferably a dictionary.

        Returns
        -------
        int of size `Py_ssize_t`
            The root dataset ID the config is registered with.

        Raises
        ------
        :class:`ManagerError`
            If there was an internal error in the dataset management. E.g. if this was called
            already before. This is to register a static config. Once.
        :class:`BrokerError`
            If there was an error in registering stuff with the broker.
        :class:`ConnectionError`
            If the broker can't be reached.

        """
        if config is None:
            raise ManagerError('The config can not be `None`.')
        if self.config_registered:
            raise ManagerError('A config was already registered, this can only be done once.')

        state_id = self._make_hash(config)
        self.logger.info('Registering config with hash {}.'.format(state_id))
        request = {'hash': state_id}
        r = requests.post(self.broker + REGISTER_STATE, data=json.dumps(request))
        r.raise_for_status()
        r = r.json()
        self._check_result(r.get('result'), REGISTER_STATE)

        # Does the broker ask for the state?
        if r.get('request') == 'get_state':
            if r.get('hash') != state_id:
                raise BrokerError('The broker is asking for state {} when state {} was registered.'
                                  .format(r.get('hash'), state_id))
            self._send_state(state_id, config)

        self.states[state_id] = config
        self.config_registered = True

        # Register a root dataset.
        ds = {'state': state_id, 'is_root': True}
        ds_id = self._make_hash(ds)
        request = {'hash': ds_id, 'ds': ds}
        r = requests.post(self.broker + REGISTER_DATASET, data=json.dumps(request))
        r.raise_for_status()
        r = r.json()
        self._check_result(r.get('result'), REGISTER_DATASET)

        self.datasets.append(ds)

    def _send_state(self, state_id, state):
        self.logger.debug('sending state {}'.format(state_id))
        request = {'hash': state_id, 'state': state}
        r = requests.post(self.broker + SEND_STATE, data=json.dumps(request))
        r.raise_for_status()
        r = r.json()
        self._check_result(r.get('result'), SEND_STATE)

    @staticmethod
    def _check_result(result, endpoint):
        if result != 'success':
            raise BrokerError('The broker answered with result `{}` to `{}`, expected `success`.'
                              .format(result, endpoint))

    @staticmethod
    def _make_hash(data):
        return hash(json.dumps(data, sort_keys=True))

    def register_state(self, name, state):
        """Register a new state with the broker.

        If the state changes, you should call this function again, passing the same name and the
        new state. For static configs that don't change, use :function:`register_config`.

        TODO: needs to keep track of states internally (use name). Rename to type?

        Parameters
        ----------
        name : str
            The name of the type of state to register.
        state : dict
            The state to register.

        Raises
        ------
        :class:`ManagerError`
            If a state was registered before the static config without initializing the manager
            with `noconfig=True`.

        """
        if self.config_registered is False and self.noconfig is False:
            raise ManagerError('Config needs to be registered before any state is added.')
        state_id = self._make_hash(state)
        ds = {'state': state_id, 'is_root': False, 'base_dset': False}
        ds_id = self._make_hash(ds)
        request = {'hash': ds_id, 'ds': ds}
        r = requests.post(self.broker, data=request)
        r.raise_for_status()
