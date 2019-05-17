"""Dump JSON to files."""
import datetime
import json
import os
from sanic.log import logger

from .manager import TIMESTAMP_FORMAT


class Dumper:
    """Take JSON, write file."""

    def __init__(self, file):
        if not os.path.isfile(file):
            try:
                open(file, 'w').close()
            except FileNotFoundError:
                logger.error("Error creating data dump file at '{}':".format(file))
                raise
        self.file = file

    def dump(self, data):
        """
        Dump json to file.

        Parameters
        ----------
        data : json
            JSON object to dump.
        """
        if 'time' not in data.keys():
            data['time'] = datetime.datetime.now().strftime(TIMESTAMP_FORMAT)
        with open(self.file, 'a') as outfile:
            json.dump(data, outfile)
            outfile.write('\n')
