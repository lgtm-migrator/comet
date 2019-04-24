import pytest
from comet import Manager, ManagerError
from comet.broker import DEFAULT_PORT

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
    manager.register_config(CONFIG)
    with pytest.raises(ManagerError):
        manager.register_config(CONFIG)

    assert CONFIG == manager.get_state()


def test_state(manager):
    global ds

    state3 = {'1': 'a', '2': 'b', '3': 'c'}
    ds = manager.register_state('abc', ABC)
    manager.register_state('abc', ABC)
    manager.register_state('123', state3)

    # Config is still the same initial one
    assert CONFIG == manager.get_state()
    assert ABC == manager.get_state('abc')

    state2 = {'a': 1, 'b': 1, 'c': 2, 'd': 3}
    manager.register_state('abc', state2)
    assert state2 == manager.get_state('abc')

    assert state3 == manager.get_state('123')

    # Add a branch to the dataset hirarchy and ask for a state using the simple schema.
    manager.register_state('abc', ABC, ds)
    with pytest.raises(ManagerError):
        manager.get_state('123')


def test_state_tree(manager):
    global ds
    assert manager.get_state('abc', ds) == ABC

    # Ignoring what has been added to the tree in `test_state`, let's build this:
    #
    # CONFIG
    #    |
    #   ABC
    #  /  |  \
    # J   H   C
    # | \
    # D  E
    #  / |
    # F  G
    ds_i = manager.register_state('foo', J, ds)
    manager.register_state('abc', H, ds)
    ds_c = manager.register_state('abc', C, ds)
    manager.register_state('abc', D, ds_i)
    ds_e = manager.register_state('abc', E, ds_i)
    ds_f = manager.register_state('abc', F, ds_e)
    ds_g = manager.register_state('abc', G, ds_e)

    # check if hashes are unique
    hashes = [ds, ds_i, ds_c, ds_e, ds_f, ds_g]
    assert len(set(hashes)) == len(hashes)

    assert manager.get_state('abc', ds_f) == F
    assert manager.get_state('abc', ds_g) == G
    assert manager.get_state('abc', ds_e) == E
    assert manager.get_state('abc', ds_c) == C
    assert manager.get_state('foo', ds_f) == J
    assert manager.get_state('foo', ds_e) == J
    assert manager.get_state('foo', ds_g) == J
    assert manager.get_state('initial_config_registered_with_coco', ds_f) == CONFIG

    # Look up a (hopefully) unknown dataset.
    with pytest.raises(ManagerError):
        manager.get_state('123', dataset_id=0)
