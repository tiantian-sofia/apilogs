"""Tests for the ``list_logs`` aggregation path in ``apilogs.core``."""
import pytest

try:
    from mock import patch
except ImportError:
    from unittest.mock import patch

from termcolor import colored

from apilogs import AWSLogs
from apilogs.core import milis2iso
from conftest import API_ID, STAGE, GATEWAY_GROUP, event

LAMBDA_A = '/aws/lambda/fxn-a'
LAMBDA_B = '/aws/lambda/fxn-b'


def lambda_integration(name):
    return {
        'type': 'AWS_PROXY',
        'uri': (
            'arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/'
            'arn:aws:lambda:us-east-1:123456789012:function:{0}/invocations'
        ).format(name),
    }


def wire_lambdas(apigateway_client, names):
    apigateway_client.get_resources.return_value = {'items': [
        {'id': 'r{0}'.format(i), 'resourceMethods': {'GET': {}}}
        for i, _ in enumerate(names)
    ]}
    def get_integration(restApiId, resourceId, httpMethod):
        index = int(resource_id_like(resourceId))
        return lambda_integration(names[index])

    def resource_id_like(resource_id):
        return ''.join(ch for ch in resource_id if ch.isdigit())

    apigateway_client.get_integration.side_effect = get_integration


def group_responses(logs_client, responses_by_group):
    """Return scripted responses for filter_log_events keyed by log group.

    Every listed response is returned once (in order); later calls for the
    group return an empty response, which exhausts (and terminates) the
    non-watch generator.
    """
    queues = dict((group, list(responses))
                  for group, responses in responses_by_group.items())

    def filter_log_events(**kwargs):
        group = kwargs['logGroupName']
        queue = queues.setdefault(group, [])
        if queue:
            return queue.pop(0)
        return {'events': []}

    logs_client.filter_log_events.side_effect = filter_log_events


def queried_groups(logs_client):
    seen = []
    for call in logs_client.filter_log_events.call_args_list:
        group = call.kwargs['logGroupName']
        if group not in seen:
            seen.append(group)
    return seen


def test_log_group_name_is_built_from_api_id_and_stage():
    # bin.py builds "API-Gateway-Execution-Logs_<api-id>/<stage>".
    with patch('boto3.client') as client_factory:
        from apilogs.bin import main
        code = main(['apilogs', 'get', '--api-id', 'xyz789', '--stage',
                     'test', '--no-color'])
    assert code == 0
    names = [call.args[0] for call in client_factory.call_args_list]
    assert 'logs' in names and 'apigateway' in names


def test_list_logs_queries_gateway_and_lambda_groups(clients, run_list_logs):
    wire_lambdas(clients['apigateway'], ['fxn-a', 'fxn-b'])
    group_responses(clients['logs'], {})
    run_list_logs()
    assert queried_groups(clients['logs']) == [GATEWAY_GROUP, LAMBDA_A,
                                               LAMBDA_B]


def test_time_window_is_passed_through(clients, run_list_logs):
    wire_lambdas(clients['apigateway'], ['fxn-a'])
    group_responses(clients['logs'], {})
    run_list_logs(start='1970-01-01 00:01:00', end='1970-01-01 00:02:00')
    for call in clients['logs'].filter_log_events.call_args_list:
        assert call.kwargs['startTime'] == 60000
        assert call.kwargs['endTime'] == 120000
        assert call.kwargs['interleaved'] is True


def test_filter_pattern_is_passed_through(clients, run_list_logs):
    wire_lambdas(clients['apigateway'], ['fxn-a'])
    group_responses(clients['logs'], {})
    run_list_logs(filter_pattern='ERROR')
    for call in clients['logs'].filter_log_events.call_args_list:
        assert call.kwargs['filterPattern'] == 'ERROR'


def test_aggregate_output_groups_and_streams(clients, run_list_logs):
    wire_lambdas(clients['apigateway'], ['fxn-a'])
    group_responses(clients['logs'], {
        GATEWAY_GROUP: [{'events': [
            event('g1', 3000, 'gateway says hi', 'gw-stream'),
        ]}],
        LAMBDA_A: [{'events': [
            event('l1', 3100, 'lambda says hi', 'lf-stream'),
        ]}],
    })
    output = run_list_logs()
    expected = (
        GATEWAY_GROUP.ljust(len(GATEWAY_GROUP)) + ' ' +
        'gw-stream'.ljust(10) + ' gateway says hi\n' +
        LAMBDA_A.ljust(len(GATEWAY_GROUP)) + ' ' +
        'lf-stream'.ljust(10) + ' lambda says hi\n'
    )
    assert output == expected


def test_no_group_no_stream(clients, run_list_logs):
    wire_lambdas(clients['apigateway'], ['fxn-a'])
    group_responses(clients['logs'], {
        GATEWAY_GROUP: [{'events': [event('g1', 1, 'hello', 'gw')]}],
    })
    output = run_list_logs(output_group_enabled=False,
                           output_stream_enabled=False)
    assert output == 'hello\n'


@pytest.mark.parametrize('flag,expected_prefix', [
    ('output_timestamp_enabled', milis2iso(1000)),
    ('output_ingestion_time_enabled', milis2iso(5000)),
])
def test_timestamp_and_ingestion_time_separately(
        clients, run_list_logs, flag, expected_prefix):
    wire_lambdas(clients['apigateway'], ['fxn-a'])
    group_responses(clients['logs'], {
        GATEWAY_GROUP: [{'events': [
            event('g1', 1000, 'first', 'gw', ingestion=5000),
        ]}],
    })
    output = run_list_logs(output_group_enabled=False,
                           output_stream_enabled=False, **{flag: True})
    assert output == '{0} first\n'.format(expected_prefix)


def test_timestamp_and_ingestion_time_together(clients, run_list_logs):
    wire_lambdas(clients['apigateway'], ['fxn-a'])
    group_responses(clients['logs'], {
        GATEWAY_GROUP: [{'events': [
            event('g1', 1000, 'first', 'gw', ingestion=5000),
        ]}],
    })
    output = run_list_logs(output_group_enabled=False,
                           output_stream_enabled=False,
                           output_timestamp_enabled=True,
                           output_ingestion_time_enabled=True)
    assert output == ('{0} {1} first\n'.format(
        milis2iso(1000), milis2iso(5000)))


def test_no_color_never_emits_escape_codes(clients, run_list_logs,
                                           force_color):
    # force_color only affects termcolor's global decision; --no-color is
    # enforced inside AWSLogs.color().
    wire_lambdas(clients['apigateway'], ['fxn-a'])
    group_responses(clients['logs'], {
        GATEWAY_GROUP: [{'events': [event('g1', 1, 'hello', 'gw')]}],
    })
    output = run_list_logs(color_enabled=False)
    assert '\x1b[' not in output
    assert output.endswith('hello\n')


def test_colored_output(clients, run_list_logs, force_color):
    wire_lambdas(clients['apigateway'], ['fxn-a'])
    group_responses(clients['logs'], {
        GATEWAY_GROUP: [{'events': [event('g1', 1, 'hello', 'gw')]}],
    })
    output = run_list_logs(color_enabled=True)
    assert output == (
        colored(GATEWAY_GROUP.ljust(len(GATEWAY_GROUP)), 'green') + ' ' +
        colored('gw'.ljust(10), 'cyan') + ' hello\n')
    assert '\x1b[32m' in output and '\x1b[36m' in output


def test_highlight_colors_matching_text(clients, run_list_logs, force_color):
    wire_lambdas(clients['apigateway'], ['fxn-a'])
    group_responses(clients['logs'], {
        GATEWAY_GROUP: [{'events': [event('g1', 1, 'an ERROR here', 'gw')]}],
    })
    output = run_list_logs(color_enabled=True, output_group_enabled=False,
                           output_stream_enabled=False,
                           highlight=['ERROR'])
    assert output == 'an {0} here\n'.format(
        colored('ERROR', 'blue', 'on_yellow'))
    assert '\x1b[34m' in output and '\x1b[43m' in output


def test_highlight_with_no_color_leaves_message_untouched(
        clients, run_list_logs):
    wire_lambdas(clients['apigateway'], ['fxn-a'])
    group_responses(clients['logs'], {
        GATEWAY_GROUP: [{'events': [event('g1', 1, 'an ERROR here', 'gw')]}],
    })
    output = run_list_logs(color_enabled=False, output_group_enabled=False,
                           output_stream_enabled=False,
                           highlight=['ERROR'])
    assert output == 'an ERROR here\n'


def test_whitespace_only_highlight_is_ignored(clients, run_list_logs):
    wire_lambdas(clients['apigateway'], ['fxn-a'])
    group_responses(clients['logs'], {
        GATEWAY_GROUP: [{'events': [event('g1', 1, 'nothing special',
                                           'gw')]}],
    })
    output = run_list_logs(output_group_enabled=False,
                           output_stream_enabled=False, highlight=['   '])
    assert output == 'nothing special\n'


def test_no_lambda_integrations_queries_only_gateway_group(
        clients, run_list_logs):
    # No resources at all: only the API Gateway group is queried.
    group_responses(clients['logs'], {})
    run_list_logs()
    assert queried_groups(clients['logs']) == [GATEWAY_GROUP]


def test_lambda_fetch_error_is_skipped_gateway_events_still_printed(
        clients, run_list_logs, caplog):
    wire_lambdas(clients['apigateway'], ['fxn-a'])

    def filter_log_events(**kwargs):
        if kwargs['logGroupName'] == LAMBDA_A:
            raise RuntimeError('ResourceNotFoundException')
        if kwargs['logGroupName'] == GATEWAY_GROUP:
            return {'events': [event('g1', 1, 'gateway ok', 'gw')]}
        return {'events': []}

    clients['logs'].filter_log_events.side_effect = filter_log_events
    with caplog.at_level('WARNING'):
        output = run_list_logs(output_group_enabled=False,
                               output_stream_enabled=False)
    assert output == 'gateway ok\n'
    assert 'Error fetching logs for Lambda function' in caplog.text


def test_deduplicates_events_seen_across_pages(clients, run_list_logs):
    wire_lambdas(clients['apigateway'], [])
    # A lingering nextToken forces a second iteration that replays the same
    # event (real CloudWatch interleaved responses do this).
    group_responses(clients['logs'], {
        GATEWAY_GROUP: [
            {'events': [event('g1', 1, 'once', 'gw')], 'nextToken': 'tok'},
            {'events': [event('g1', 1, 'once', 'gw')]},
        ],
    })
    output = run_list_logs(output_group_enabled=False,
                           output_stream_enabled=False)
    assert output == 'once\n'


def test_next_token_is_returned_on_following_request(clients, run_list_logs):
    wire_lambdas(clients['apigateway'], [])
    group_responses(clients['logs'], {
        GATEWAY_GROUP: [
            {'events': [event('g1', 1, 'first', 'gw')], 'nextToken': 'tok-1'},
            {'events': [event('g2', 2, 'second', 'gw')], 'nextToken': 'tok-2'},
            {'events': []},
        ],
    })
    run_list_logs()
    calls = clients['logs'].filter_log_events.call_args_list
    assert 'nextToken' not in calls[0].kwargs
    assert calls[1].kwargs['nextToken'] == 'tok-1'
    assert calls[2].kwargs['nextToken'] == 'tok-2'


@pytest.mark.xfail(strict=True, reason=(
    'list_logs calls sorted(allevents, key=timestamp) but discards the '
    'return value, so merged gateway/lambda events print in call order '
    '(gateway first) instead of chronological order.'))
def test_merged_events_are_printed_in_timestamp_order(
        clients, run_list_logs):
    wire_lambdas(clients['apigateway'], ['fxn-a'])
    group_responses(clients['logs'], {
        GATEWAY_GROUP: [{'events': [event('g1', 3000, 'late gateway',
                                           'gw')]}],
        LAMBDA_A: [{'events': [event('l1', 1000, 'early lambda',
                                     'lf')]}],
    })
    output = run_list_logs(output_group_enabled=False,
                           output_stream_enabled=False)
    assert output.splitlines() == ['early lambda', 'late gateway']
