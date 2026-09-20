"""Shared fixtures for the apilogs test-suite.

Boto3 is fully mocked: ``boto3.client('logs')`` and
``boto3.client('apigateway')`` return in-memory mocks, so no test ever
talks to AWS and no third-party mocking library (moto, etc.) is needed.
"""
import threading

import pytest

try:
    from mock import patch, Mock
except ImportError:
    from unittest.mock import patch, Mock

from apilogs.core import AWSLogs

API_ID = 'abc123'
STAGE = 'prod'
GATEWAY_GROUP = 'API-Gateway-Execution-Logs_{0}/{1}'.format(API_ID, STAGE)


def event(event_id, timestamp, message, stream, ingestion=None):
    return {
        'eventId': str(event_id),
        'timestamp': timestamp,
        'ingestionTime': timestamp + 1 if ingestion is None else ingestion,
        'message': message,
        'logStreamName': stream,
    }


@pytest.fixture
def clients():
    """Return the two mocked boto3 clients used by ``AWSLogs``."""
    logs_client = Mock()
    apigateway_client = Mock()

    # By default every log group is empty and never paginates, so a single
    # filter_log_events() iteration exhausts the group.
    logs_client.filter_log_events.return_value = {'events': []}
    logs_client.get_paginator.return_value.paginate.return_value = iter([])
    apigateway_client.get_resources.return_value = {'items': []}

    def make_client(name, **kwargs):
        if name == 'logs':
            return logs_client
        if name == 'apigateway':
            return apigateway_client
        raise AssertionError('unexpected boto3 client: {0}'.format(name))

    with patch('boto3.client', side_effect=make_client):
        yield {'logs': logs_client, 'apigateway': apigateway_client}


@pytest.fixture
def run_list_logs(clients):
    """Run ``AWSLogs.list_logs`` capturing stdout and return the output.

    Synchronisation with the internal producer/consumer threads is done with
    ``Thread.join`` on the exact threads spawned by the call -- no polling or
    sleeps. list_logs() itself only returns once its ``exit`` Event is set by
    the consumer; a leftover (still spinning) producer thread is reported as
    an error instead of being left dangling into the next test.
    """
    def run(**overrides):
        options = dict(
            aws_region='us-east-1',
            aws_access_key_id=None,
            aws_secret_access_key=None,
            aws_session_token=None,
            log_group_name=GATEWAY_GROUP,
            api_id=API_ID,
            stage=STAGE,
            log_stream_name='ALL',
            filter_pattern=None,
            highlight=None,
            watch=False,
            color_enabled=False,
            output_stream_enabled=True,
            output_group_enabled=True,
            output_timestamp_enabled=False,
            output_ingestion_time_enabled=False,
            start=None,
            end=None,
        )
        options.update(overrides)

        awslogs = AWSLogs(**options)
        threads_before = set(threading.enumerate())
        with patch('sys.stdout') as fake_stdout:
            awslogs.list_logs()
            spawned = [t for t in threading.enumerate()
                       if t not in threads_before and t.is_alive()]
            for thread in spawned:
                thread.join(timeout=5)
            assert all(not t.is_alive() for t in spawned), (
                'list_logs worker threads did not terminate')
            written = ''.join(
                call_arg.args[0]
                for call_arg in fake_stdout.write.call_args_list
                if call_arg.args
            )
        return written

    return run


@pytest.fixture
def force_color(monkeypatch):
    """Force termcolor to emit ANSI codes regardless of NO_COLOR/tty."""
    import termcolor

    monkeypatch.delenv('NO_COLOR', raising=False)
    monkeypatch.delenv('ANSI_COLORS_DISABLED', raising=False)
    monkeypatch.setenv('FORCE_COLOR', '1')
    termcolor.can_colorize.cache_clear()
    yield
    termcolor.can_colorize.cache_clear()
