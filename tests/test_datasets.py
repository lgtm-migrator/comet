import pytest
from comet import Manager, ManagerError
from comet.broker import DEFAULT_PORT


@pytest.fixture(scope='session', autouse=True)
def manager():
    return Manager('localhost', DEFAULT_PORT)


def test_hash(manager):
    assert isinstance(manager, Manager)

    data1 = {'a': 1, 'b': 'fubar'}
    data2 = {'a': 1, 'b': 'fubar'}
    data3 = {'a': 1, 'b': 'fuba'}
    data4 = {'a': 1, 'c': 'fubar'}
    data5 = {'a': 2, 'b': 'fubar'}

    assert manager._make_hash(data1) == manager._make_hash(data1)
    assert manager._make_hash(data1) == manager._make_hash(data2)
    assert manager._make_hash(data1) != manager._make_hash(data3)
    assert manager._make_hash(data1) != manager._make_hash(data4)
    assert manager._make_hash(data1) != manager._make_hash(data5)


def test_register_config_twice(manager):
    data1 = {'a': 1, 'b': 'fubar'}
    manager.register_config(data1)
    with pytest.raises(ManagerError):
        manager.register_config(data1)
