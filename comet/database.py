"""Database model for comet."""

import datetime
import logging
import json
import peewee

from .manager import TIMESTAMP_FORMAT, LOG_FORMAT

mysql_db = peewee.MySQLDatabase(None)

logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger("comet.database")
logger.setLevel('INFO')


class DatasetStateType(peewee.Model):
    """Model for datasetstatetype table."""

    name = peewee.CharField()

    class Meta:
        """Connect model to database."""

        database = mysql_db


class DatasetState(peewee.Model):
    """Model for datasetstate table."""

    class LongTextField(peewee.TextField):
        """Peewee field supporting MySQL longtext."""

        field_type = 'LONGTEXT'

    class LongJSONDictField(LongTextField):
        """Fake JSON dict field.

        In theory MySQL has a native JSON field, but peewee does not support it.

        Note: using json.dumps(value) slows down archiving by factor ~10.

        Adapted from ch_util (Author: richard@phas.ubc.ca).
        """

        def db_value(self, value):
            """Serialize the python values for storage in the db."""
            if value is None:
                return None

            if not isinstance(value, dict):
                raise ValueError("State data must be a dict. Received {}".format(type(value)))

            return json.dumps(value)

        def python_value(self, value):
            """Deserialize the DB string to JSON."""
            if value is None:
                return None

            pyval = json.loads(value)

            if not isinstance(pyval, dict):
                raise ValueError("State data must convert to dict. Got {}".format(type(pyval)))

            return pyval

    id = peewee.DecimalField(21, 0, primary_key=True)
    type = peewee.ForeignKeyField(DatasetStateType, null=True)
    data = LongJSONDictField()

    class Meta:
        """Connect model to database."""

        database = mysql_db


class Dataset(peewee.Model):
    """Model for dataset table."""

    id = peewee.DecimalField(21, 0, primary_key=True)
    root = peewee.BooleanField()
    state = peewee.ForeignKeyField(DatasetState)
    time = peewee.DateTimeField()
    base_dset = peewee.ForeignKeyField("self", null=True)

    class Meta:
        """Connect model to database."""

        database = mysql_db


class DatasetAttachedType(peewee.Model):
    """
    Model for datasettypes table.

    Lists which state types are attached to which datasets
    """

    id = peewee.AutoField(primary_key=True)
    dataset_id = peewee.ForeignKeyField(Dataset)
    type = peewee.ForeignKeyField(DatasetStateType)

    class Meta:
        """Connect model to database."""

        database = mysql_db


class DatasetCurrentState(peewee.Model):
    """Model for datasetcurrentstate table."""

    state = peewee.ForeignKeyField(DatasetState)
    time = peewee.DateTimeField()

    class Meta:
        """Connect model to database."""

        database = mysql_db


class Database:
    """A peewee database model describing states, datasets and their relations."""

    def __init__(self, db_name, db_user, db_passwd, db_host, db_port):
        mysql_db.init(db_name, user=db_user, password=db_passwd, host=db_host, port=db_port)
        mysql_db.connect()

        # Create any missing table.
        mysql_db.create_tables([DatasetState, DatasetCurrentState, Dataset, DatasetStateType,
                                DatasetAttachedType])

    @staticmethod
    def insert_state(entry):
        """
        Insert a dataset state.

        Parameters
        ----------
        entry : dict
            A dict containing the fields `state`, `hash` and `time`. If `state` is `None` this will
            add an entry to the datasetcurrentstate entry with a reference to the state using it's
            ID or return `False` in case the state is not found.

        Returns
        -------
            `True` if successful, `False` if "entry[`state`]" is `False` but the state references
            by `entry["hash"]` is not in the database.
        """
        if entry["state"] is None:
            try:
                state = DatasetState.get(DatasetState.id == entry["hash"])
            except DatasetState.DoesNotExist:
                return False
            DatasetCurrentState.get_or_create(
                state=state,
                time=datetime.datetime.strptime(entry["time"], TIMESTAMP_FORMAT))
        else:
            # Make sure state type known to DB
            state_type, _ = DatasetStateType.get_or_create(name=entry["state"]["type"])

            # Add this state to the DB
            DatasetState.get_or_create(id=entry["hash"], type=state_type, data=entry["state"])
        return True

    @staticmethod
    def insert_dataset(entry):
        """
        Insert a dataset.

        Parameters
        ----------
        entry : dict
            A dict containing the fields `ds/state`, `ds/is_root`, `ds/types`, `time`, `hash`.
        """
        state = DatasetState.get(DatasetState.id == entry["ds"]["state"])
        try:
            dataset = Dataset.get(Dataset.id == entry["hash"])
        except Dataset.DoesNotExist:
            dataset, _ = Dataset.get_or_create(
                id=entry["hash"], state=state, root=entry["ds"]["is_root"],
                time=datetime.datetime.strptime(entry["time"], TIMESTAMP_FORMAT))

        for state_type in entry["ds"]["types"]:
            state_type_id, _ = DatasetStateType.get_or_create(name=state_type)
            DatasetAttachedType.get_or_create(dataset_id=dataset, type=state_type_id)

    @staticmethod
    def get_dataset(id):
        """
        Get dataset.

        Parameters
        ----------
        id : int
            The ID of the dataset to get.

        Returns
        -------
        The dataset or `None` if not found.
        """
        try:
            return Dataset.get(Dataset.id == id)
        except Dataset.DoesNotExist:
            logger.warning("Could not find dataset {}.".format(id))
            return None

    @staticmethod
    def get_state(id):
        """
        Get dataset state.

        Parameters
        ----------
        id : int
            ID of state to get.

        Returns
        -------
        :class:'State'
            The dataset state, or `None` if not found.
        """
        try:
            return DatasetState.get(DatasetState.id == id)
        except DatasetState.DoesNotExist:
            logger.warning("Could not find state {}.".format(id))
            return None

    @staticmethod
    def get_types(dataset_id):
        """
        Get state types of the given dataset.

        Parameters
        ----------
        dataset_id : int
            ID of the dataset to find attached state types for.

        Returns
        -------
        list of str
            The state types attached to the dataset. Returns `None` if dataset or types not found.
        """
        try:
            result = DatasetAttachedType.select().where(
                DatasetAttachedType.dataset_id == dataset_id)
        except DatasetAttachedType.DoesNotExist:
            logger.warning("Could not find any types attached to dataset {}.".format(dataset_id))
            return None
        types = list()
        for t in result:
            types.append(t.type.name)
        return types
