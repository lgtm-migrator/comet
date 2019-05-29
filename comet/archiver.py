"""
Archiver for CoMeT.

Moves comet broker dumps to a MySQL database
"""

import asyncio
import datetime
import json
import os
import peewee
import signal
import warnings

from time import sleep
import logging
from . import Manager, CometError, __version__
from .broker import DEFAULT_PORT
from .manager import TIMESTAMP_FORMAT

# _STATE_DIR = "/var/lib/comet-archiver"
LOG_FORMAT = '[%(asctime)s] %(name)s: %(message)s'


mysql_db = peewee.MySQLDatabase(None)
logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger("comet.archiver")
logger.setLevel('INFO')


class DatasetState(peewee.Model):
    """Model for datasetstate table."""

    class LongTextField(peewee.TextField):
        """Peewee field supporting MySQL longtext."""

        field_type = 'LONGTEXT'

    hash = peewee.DecimalField(21, 0, primary_key=True)
    type = peewee.CharField()
    data = LongTextField()

    class Meta:
        """Connect model to database."""

        database = mysql_db


class DatasetCurrentState(peewee.Model):
    """Model for datasetcurrentstate table."""

    id = peewee.AutoField()
    hash = peewee.DecimalField(21, 0)
    time = peewee.DateTimeField()

    class Meta:
        """Connect model to database."""

        database = mysql_db


class DatasetStateType(peewee.Model):
    """Model for datasetstatetype table."""

    id = peewee.AutoField()
    name = peewee.CharField()

    class Meta:
        """Connect model to database."""

        database = mysql_db


class Dataset(peewee.Model):
    """Model for dataset table."""

    hash = peewee.DecimalField(21, 0, primary_key=True)
    root = peewee.BooleanField()
    state = peewee.BigIntegerField()
    time = peewee.DateTimeField()
    types = peewee.CharField()

    class Meta:
        """Connect model to database."""

        database = mysql_db


def _insert_state(entry):
    if entry["state"] is None:
        # Add a row to dataset_state_current
        DatasetCurrentState.insert(
            {
                "hash": entry["hash"],
                "time": datetime.datetime.strptime(
                    entry["time"], TIMESTAMP_FORMAT),
            }).on_conflict_replace().execute()
    else:
        # Check if state type known to DB
        type = entry["state"]["type"]
        DatasetStateType.insert({"name": type}).on_conflict_ignore().execute()
        # Add this state to the DB
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore")
            DatasetState.insert(
                {
                    "hash": entry["hash"],
                    "type": type,
                    "data": entry["state"],
                }
            ).on_conflict_ignore().execute()


def _insert_dataset(entry):
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore")
        Dataset.insert(
            {
                "hash": entry["hash"],
                "state": entry["ds"]["state"],
                "root": entry["ds"]["is_root"],
                "types": entry["ds"]["types"],
                "time": entry["time"],
            }
        ).on_conflict_ignore().execute()


class Archiver():
    """Main class to run the comet archiver."""

    def __init__(self, data_dump_path, scrape_interval, db_name, db_host, db_port, db_user,
                 db_passwd):

        startup_time = datetime.datetime.utcnow()
        config = {"data_dump_path": data_dump_path, "scrape_interval": scrape_interval}
        self.dir = data_dump_path
        self.interval = scrape_interval

        sleep(1)
        manager = Manager("localhost", DEFAULT_PORT)
        try:
            manager.register_start(startup_time, __version__)
            manager.register_config(config)
        except CometError as exc:
            logger.error('Comet archiver failed registering its startup and initial config: {}'
                         .format(exc))
            exit(1)

        self.loop = asyncio.get_event_loop()
        self.task = self.loop.create_task(self._scrape())

        # Open database connection
        mysql_db.init(db_name, user=db_user, password=db_passwd, host=db_host, port=db_port)
        mysql_db.connect()

        # Create any missing table.
        mysql_db.create_tables([DatasetState, DatasetCurrentState, Dataset, DatasetStateType])

    def run(self):
        """Run comet archiver."""
        logger.info("Started CoMeT Archiver ({}), scraping path '{}' every {}."
                    .format(__version__, self.dir,
                            datetime.timedelta(seconds=self.interval)))

        signal.signal(signal.SIGINT, lambda s, f: self.loop.stop())

        try:
            self.loop.run_forever()
        except asyncio.CancelledError:
            pass

    async def _scrape(self):
        try:
            time_last_scraped = datetime.datetime.min
            while True:

                # Find the dumped files
                dump_files = os.listdir(self.dir)
                dump_files = list(filter(lambda x: x.endswith("data.dump"), dump_files))
                dump_times = [f[:-10] for f in dump_files]
                dump_times = [datetime.datetime.strptime(t, TIMESTAMP_FORMAT) for t in dump_times]
                dump_times, dump_files = zip(*sorted(zip(dump_times, dump_files)))

                new_times = list(filter(lambda t: t > time_last_scraped, dump_times))

                for time in new_times:
                    dfile = dump_files[dump_times.index(time)]

                    # Only read files that don't have a lock file.
                    if not os.path.isfile(os.path.join(self.dir, "{}.lock".format(dfile))):
                        # Set time_last_scraped to the timestamp of this file
                        time_last_scraped = time
                        with open(os.path.join(self.dir, dfile), 'r') as json_file:
                            line_num = 0
                            for line in json_file:
                                line_num += 1
                                entry = json.loads(line)
                                logger.debug("Archiving {}".format(entry))
                                if "state" in entry.keys():
                                    try:
                                        _insert_state(entry)
                                    except KeyError as key:
                                        logger.error("Entry in dump file {}:{} is missing key {}. "
                                                     "Skipping! This is the entry:\n{}"
                                                     .format(os.path.join(self.dir, dfile),
                                                             line_num, key, entry))
                                elif "ds" in entry.keys():
                                    try:
                                        _insert_dataset(entry)
                                    except KeyError as key:
                                        logger.error("Entry in dump file {}:{} is missing key {}. "
                                                     "Skipping! This is the entry:\n{}"
                                                     .format(os.path.join(self.dir, dfile),
                                                             line_num, key, entry))
                                else:
                                    logger.warn("Entry in dump file {}:{} is neither a state "
                                                "nor a dataset. Skipping! This is the entry:\n{}"
                                                .format(os.path.join(self.dir, dfile), line_num,
                                                        entry))
                        logger.info("Archived {} entries from {}".format(line_num, dfile))

                logger.debug('Scraping again in {}.'
                             .format(datetime.timedelta(seconds=self.interval)))
                await asyncio.sleep(self.interval)
        except BaseException:
            self.loop.stop()
            raise

    def stop(self):
        """Stop the archiver."""
        self.task.cancel()
