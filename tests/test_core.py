import re
from datetime import datetime
from unittest.mock import patch, Mock

import pytest
from botocore.compat import total_seconds
from termcolor import colored

from apilogs import AWSLogs
from apilogs.core import milis2iso
from apilogs.exceptions import UnknownDateError

API_ID = 'abc123'
STAGE = 'prod'
LOG_GROUP = 'API-Gateway-Execution-Logs_{0}/{1}'.format(API_ID, STAGE)

LIST_LOGS_DEFAULTS = dict(
    api_id=API_ID,
    stage=STAGE,
    log_group_name=LOG_GROUP,
    log_stream_name='ALL',
    color_enabled=False,
    output_group_enabled=True,
    output_stream_enabled=True,
    output_timestamp_enabled=False,
    output_ingestion_time_enabled=False,
    watch=False,
)


def make_apilogs(logs_client=None, apig_client=None, **kwargs):
    """Build an AWSLogs instance with mocked boto3 logs/apigateway clients."""
    logs_client = logs_client or Mock()
    apig_client = apig_client or Mock()
    clients = {'logs': logs_client, 'apigateway': apig_client}
    with patch('boto3.client', side_effect=lambda service, **kw: clients[service]):
        instance = AWSLogs(**kwargs)
    return instance, logs_client, apig_client


def make_event(event_id, message, timestamp, stream='stream-1', ingestion=None):
    return {
        'eventId': event_id,
        'message': message,
        'timestamp': timestamp,
        'ingestionTime': timestamp if ingestion is None else ingestion,
        'logStreamName': stream,
    }


def lambda_uri(function_name):
    return ('arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/'
            'arn:aws:lambda:us-east-1:123456789012:function:{0}'
            '/invocations'.format(function_name))


def make_apig_client(integrations):
    """Mock apigateway client serving one GET integration per resource.

    ``integrations`` is a list of ``(type, uri)`` tuples. Side effects are
    function-based so repeated polling rounds (the generator thread may poll
    more than once before the consumer drains the queue) never exhaust them.
    """
    client = Mock()
    by_resource = {}
    items = []
    for index, (itype, uri) in enumerate(integrations):
        resource_id = 'res{0}'.format(index)
        items.append({'id': resource_id, 'resourceMethods': {'GET': {}}})
        by_resource[resource_id] = {'type': itype, 'uri': uri}
    client.get_resources.return_value = {'items': items}
    client.get_integration.side_effect = (
        lambda restApiId, resourceId, httpMethod: by_resource[resourceId])
    return client


def make_logs_client(events_by_group):
    """Mock logs client whose filter_log_events serves ``events_by_group``."""
    client = Mock()

    def fake_filter(**kwargs):
        # Copy events: list_logs mutates them (adds 'group_name') and the
        # generator may fetch the same group more than once per run.
        events = [dict(e) for e in events_by_group.get(kwargs['logGroupName'], [])]
        return {'events': events}

    client.filter_log_events.side_effect = fake_filter
    return client


def run_list_logs(capsys, events_by_group, integrations, **overrides):
    """Run list_logs fully mocked and return captured stdout lines.

    Deterministic without sleeps: in non-watch mode the generator thread
    enqueues a None sentinel once no next_tokens remain, the consumer thread
    then sets the exit event and list_logs returns. Repeated polling rounds
    are harmless because event ids are deduplicated by the LRU queue.
    """
    logs_client = make_logs_client(events_by_group)
    apig_client = make_apig_client(integrations)
    kwargs = dict(LIST_LOGS_DEFAULTS)
    kwargs.update(overrides)
    logs, _, _ = make_apilogs(logs_client, apig_client, **kwargs)
    logs.list_logs()
    return capsys.readouterr().out.splitlines(), logs_client


class TestLogGroupName(object):
    def test_group_name_built_from_api_id_and_stage(self, capsys):
        events = {LOG_GROUP: [make_event('e1', 'hello', 1000)]}
        lines, logs_client = run_list_logs(capsys, events, [])
        assert len(lines) == 1
        assert lines[0].startswith(LOG_GROUP)
        queried_groups = {c[1]['logGroupName']
                          for c in logs_client.filter_log_events.call_args_list}
        assert queried_groups == {LOG_GROUP}


class TestGetLambdaFunctionNames(object):
    def names_for(self, integrations):
        apig_client = make_apig_client(integrations)
        logs, _, _ = make_apilogs(apig_client=apig_client)
        return logs.get_lambda_function_names(API_ID, STAGE)

    def test_aws_and_aws_proxy_integrations(self):
        names = self.names_for([
            ('AWS_PROXY', lambda_uri('proxy-func')),
            ('AWS', lambda_uri('aws-func')),
        ])
        assert sorted(names) == ['aws-func', 'proxy-func']

    def test_function_name_with_alias(self):
        names = self.names_for([('AWS_PROXY', lambda_uri('my-func:live'))])
        assert names == ['my-func:live']

    def test_non_lambda_integrations_are_skipped(self):
        names = self.names_for([
            ('HTTP', 'http://example.com/backend'),
            ('MOCK', ''),
            ('AWS', 'arn:aws:apigateway:us-east-1:dynamodb:action/Scan'),
        ])
        assert names == []

    def test_lambda_log_group_strips_alias(self, capsys):
        group = '/aws/lambda/my-func'
        events = {group: [make_event('e1', 'aliased', 1000)]}
        lines, logs_client = run_list_logs(
            capsys, events, [('AWS_PROXY', lambda_uri('my-func:live'))])
        queried_groups = {c[1]['logGroupName']
                          for c in logs_client.filter_log_events.call_args_list}
        assert group in queried_groups
        assert '/aws/lambda/my-func:live' not in queried_groups
        assert any('aliased' in line for line in lines)


class TestListLogsAggregation(object):
    def test_gateway_and_multiple_lambda_groups_aggregated(self, capsys):
        events = {
            LOG_GROUP: [make_event('gw-1', 'gateway event', 1000)],
            '/aws/lambda/fn-a': [make_event('a-1', 'lambda a event', 2000)],
            '/aws/lambda/fn-b': [make_event('b-1', 'lambda b event', 3000)],
        }
        integrations = [('AWS_PROXY', lambda_uri('fn-a')),
                        ('AWS_PROXY', lambda_uri('fn-b'))]
        lines, logs_client = run_list_logs(capsys, events, integrations)

        assert len(lines) == 3
        by_message = {}
        for line in lines:
            for message in ('gateway event', 'lambda a event', 'lambda b event'):
                if message in line:
                    by_message[message] = line
        assert set(by_message) == {'gateway event', 'lambda a event',
                                   'lambda b event'}
        assert LOG_GROUP in by_message['gateway event']
        assert '/aws/lambda/fn-a' in by_message['lambda a event']
        assert '/aws/lambda/fn-b' in by_message['lambda b event']

        queried_groups = {c[1]['logGroupName']
                          for c in logs_client.filter_log_events.call_args_list}
        assert queried_groups == {LOG_GROUP, '/aws/lambda/fn-a',
                                  '/aws/lambda/fn-b'}
        for call in logs_client.filter_log_events.call_args_list:
            assert call[1]['interleaved'] is True

    def test_duplicate_events_are_printed_once(self, capsys):
        # The generator may poll a group more than once before the consumer
        # drains the queue; the LRU dedup must drop repeated event ids.
        events = {LOG_GROUP: [make_event('gw-1', 'only once', 1000)]}
        lines, _ = run_list_logs(capsys, events, [])
        assert sum('only once' in line for line in lines) == 1

    @pytest.mark.xfail(
        reason=('core.py generator calls sorted(allevents, ...) but discards '
                'the result, so events are printed in fetch order (API '
                'Gateway group first, then each Lambda group) instead of '
                'being interleaved by timestamp'),
        strict=True)
    def test_events_printed_in_timestamp_order(self, capsys):
        events = {
            LOG_GROUP: [make_event('gw-1', 'gateway late', 3000)],
            '/aws/lambda/fn-a': [make_event('a-1', 'lambda early', 1000)],
        }
        integrations = [('AWS_PROXY', lambda_uri('fn-a'))]
        lines, _ = run_list_logs(capsys, events, integrations)
        assert len(lines) == 2
        assert 'lambda early' in lines[0]
        assert 'gateway late' in lines[1]


class TestOutputFormatting(object):
    def test_highlight_colors_match(self, capsys):
        events = {LOG_GROUP: [make_event('e1', 'an ERROR happened', 1000)]}
        lines, _ = run_list_logs(capsys, events, [],
                                 color_enabled=True, highlight=['ERROR'])
        assert len(lines) == 1
        expected = colored('ERROR', 'blue', 'on_yellow')
        assert 'an {0} happened'.format(expected) in lines[0]

    def test_highlight_with_no_color_stays_plain(self, capsys):
        events = {LOG_GROUP: [make_event('e1', 'an ERROR happened', 1000)]}
        lines, _ = run_list_logs(capsys, events, [],
                                 color_enabled=False, highlight=['ERROR'])
        assert lines == ['{0} {1} an ERROR happened'.format(
            LOG_GROUP, 'stream-1'.ljust(10))]
        assert '\x1b[' not in lines[0]

    def test_timestamp_and_ingestion_time_columns(self, capsys):
        events = {LOG_GROUP: [make_event('e1', 'timed', 1000, ingestion=2000)]}
        lines, _ = run_list_logs(capsys, events, [],
                                 output_timestamp_enabled=True,
                                 output_ingestion_time_enabled=True)
        assert len(lines) == 1
        assert milis2iso(1000) in lines[0]
        assert milis2iso(2000) in lines[0]
        assert lines[0].endswith('timed')

    def test_timestamp_and_ingestion_time_hidden_by_default(self, capsys):
        events = {LOG_GROUP: [make_event('e1', 'timed', 1000, ingestion=2000)]}
        lines, _ = run_list_logs(capsys, events, [])
        assert milis2iso(1000) not in lines[0]
        assert milis2iso(2000) not in lines[0]


class TestParseDatetime(object):
    @pytest.fixture
    def logs(self):
        instance, _, _ = make_apilogs()
        return instance

    @staticmethod
    def epoch_ms(year, month, day, hour=0, minute=0, second=0):
        dt = datetime(year, month, day, hour, minute, second)
        return int(total_seconds(dt - datetime(1970, 1, 1))) * 1000

    def test_empty_values(self, logs):
        assert logs.parse_datetime('') is None
        assert logs.parse_datetime(None) is None

    @pytest.mark.parametrize('text,seconds', [
        ('1m', 60), ('1m ago', 60), ('1minute', 60), ('1minute ago', 60),
        ('1minutes', 60), ('1minutes ago', 60), ('5m', 300),
        ('1h', 3600), ('1h ago', 3600), ('1hour', 3600), ('1hour ago', 3600),
        ('1hours', 3600), ('1hours ago', 3600), ('2 hours ago', 7200),
        ('1d', 86400), ('1d ago', 86400), ('1day', 86400),
        ('1day ago', 86400), ('1days', 86400), ('1days ago', 86400),
        ('1w', 604800), ('1w ago', 604800), ('1week', 604800),
        ('1week ago', 604800), ('1weeks', 604800), ('1weeks ago', 604800),
    ])
    def test_relative_times(self, logs, text, seconds):
        with patch('apilogs.core.datetime') as datetime_mock:
            datetime_mock.utcnow.return_value = datetime(2015, 1, 1, 3, 0, 0)
            datetime_mock.side_effect = lambda *a, **kw: datetime(*a, **kw)
            expected = self.epoch_ms(2015, 1, 1, 3) - seconds * 1000
            assert logs.parse_datetime(text) == expected

    @pytest.mark.parametrize('text,expected', [
        ('2013-01-01', epoch_ms.__func__(2013, 1, 1)),
        ('1/1/2012 12:34', epoch_ms.__func__(2012, 1, 1, 12, 34)),
        ('1/1/2011 12:34:56', epoch_ms.__func__(2011, 1, 1, 12, 34, 56)),
    ])
    def test_absolute_times(self, logs, text, expected):
        assert logs.parse_datetime(text) == expected

    def test_unknown_date_raises(self, logs):
        with pytest.raises(UnknownDateError):
            logs.parse_datetime('not a date at all')
