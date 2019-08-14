"""Utility functions for comet."""

import datetime


def datetime_to_float(d):
    """
    Convert datetime to float (seconds).

    Parameters
    ----------
    d : datetime
        :class:`datetime` object to convert.

    Returns
    -------
    float
        The datetime in seconds.
    """
    epoch = datetime.datetime.utcfromtimestamp(0)
    total_seconds = (d - epoch).total_seconds()
    # total_seconds will be in decimals (millisecond precision)
    return total_seconds


def float_to_datetime(fl):
    """
    Convert a float (seconds) to datetime.

    Parameters
    ----------
    fl : float
        Seconds to convert.

    Returns
    -------
    :class:`datetime`
        The seconds converted to datetime.
    """
    return datetime.datetime.utcfromtimestamp(fl)
