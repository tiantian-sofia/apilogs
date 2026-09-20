"""Tests for ``AWSLogs.parse_datetime`` (the --start/--end parsing)."""
from datetime import datetime

import pytest

try:
    from mock import patch
except ImportError:
    from unittest.mock import patch

from botocore.compat import total_seconds

from apilogs import AWSLogs
from apilogs.exceptions import UnknownDateError

NOW = datetime(2015, 1, 1, 3, 0, 0)
EPOCH = datetime(1970, 1, 1)


def epoch_ms(iso_string):
    return int(total_seconds(
        datetime.strptime(iso_string, '%Y-%m-%d %H:%M:%S') - EPOCH)) * 1000


@pytest.fixture
def awslogs(clients):
    with patch('apilogs.core.datetime') as datetime_mock:
        datetime_mock.utcnow.return_value = NOW
        datetime_mock.return_value = EPOCH
        yield AWSLogs()


@pytest.mark.parametrize('value', [None, ''])
def test_empty_input_returns_none(awslogs, value):
    assert awslogs.parse_datetime(value) is None


@pytest.mark.parametrize('text,expected', [
    # minutes
    ('1m', '2015-01-01 02:59:00'),
    ('1m ago', '2015-01-01 02:59:00'),
    ('1minute', '2015-01-01 02:59:00'),
    ('1minutes', '2015-01-01 02:59:00'),
    ('2m', '2015-01-01 02:58:00'),
    # hours
    ('1h', '2015-01-01 02:00:00'),
    ('1h ago', '2015-01-01 02:00:00'),
    ('1hour', '2015-01-01 02:00:00'),
    ('2hours', '2015-01-01 01:00:00'),
    # days
    ('1d', '2014-12-31 03:00:00'),
    ('1d ago', '2014-12-31 03:00:00'),
    ('1day', '2014-12-31 03:00:00'),
    ('2days', '2014-12-30 03:00:00'),
    # weeks
    ('1w', '2014-12-25 03:00:00'),
    ('1w ago', '2014-12-25 03:00:00'),
    ('1week', '2014-12-25 03:00:00'),
    ('2weeks', '2014-12-18 03:00:00'),
])
def test_relative_times(awslogs, text, expected):
    assert awslogs.parse_datetime(text) == epoch_ms(expected)


@pytest.mark.parametrize('text,expected', [
    ('2015-01-01 02:59:00', '2015-01-01 02:59:00'),
    ('1/1/2013', '2013-01-01 00:00:00'),
    ('1/1/2012 12:34', '2012-01-01 12:34:00'),
    ('1/1/2011 12:34:56', '2011-01-01 12:34:56'),
])
def test_absolute_times(awslogs, text, expected):
    assert awslogs.parse_datetime(text) == epoch_ms(expected)


def test_unknown_text_raises(awslogs):
    with pytest.raises(UnknownDateError):
        awslogs.parse_datetime('???')
