import json
import pytest

from datetime import datetime, timedelta
from comet import Manager, ManagerError
from comet.broker import DEFAULT_PORT
from comet.manager import TIMESTAMP_FORMAT

# Some dummy states for testing:
CONFIG = {'a': 1, 'b': 'fubar'}
ABC = {'a': 0, 'b': 1, 'c': 2, 'd': 3}
A = {'a': 1, 'b': 'fubar'}
B = {'a': 1, 'b': 'fubar'}
C = {'a': 1, 'b': 'fuba'}
D = {'a': 1, 'c': 'fubar'}
E = {'a': 2, 'b': 'fubar'}
F = {'a': 1}
G = {'b': 1}
H = {'blubb': 'bla'}
J = {'meta': 'data'}


@pytest.fixture(scope='session', autouse=True)
def manager():
    return Manager('localhost', DEFAULT_PORT)


def test_hash(manager):
    assert isinstance(manager, Manager)

    assert manager._make_hash(A) == manager._make_hash(A)
    assert manager._make_hash(A) == manager._make_hash(B)
    assert manager._make_hash(A) != manager._make_hash(C)
    assert manager._make_hash(A) != manager._make_hash(D)
    assert manager._make_hash(A) != manager._make_hash(E)


def test_register_config(manager):
    now = datetime.now()
    version = '0.1.1'

    with pytest.raises(ManagerError):
        manager.register_config(CONFIG)
    manager.register_start(now, version)
    manager.register_config(CONFIG)
    with pytest.raises(ManagerError):
        manager.register_start(now, version)

    assert CONFIG == manager.get_state()

    with open('data.dump', 'r') as json_file:
        start_dump = json.loads(json_file.readline())
        config_dump = json.loads(json_file.readline())

    expected_start_dump = {'time': now.strftime(TIMESTAMP_FORMAT), 'version': version,
                           'name': __name__, 'type': 'start'}
    assert start_dump['state'] == expected_start_dump
    assert start_dump['hash'] == manager._make_hash(expected_start_dump)
    assert datetime.strptime(start_dump['time'], TIMESTAMP_FORMAT) - datetime.now() < timedelta(
        minutes=1)
    assert datetime.strptime(start_dump['state']['time'],
                             TIMESTAMP_FORMAT) - datetime.now() < timedelta(minutes=1)

    assert config_dump['state'] == CONFIG
    assert datetime.strptime(config_dump['time'], TIMESTAMP_FORMAT) - datetime.now() < timedelta(
        minutes=1)
    assert config_dump['hash'] == manager._make_hash(CONFIG)
