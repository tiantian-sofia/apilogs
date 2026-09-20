"""End-to-end-ish tests through ``apilogs.bin.main`` with boto3 mocked.

Note: main() applies argv[1:] itself (as it does to sys.argv), so every
command list passed here carries the program name first.
"""
import pytest

try:
    from mock import patch, Mock
except ImportError:
    from unittest.mock import patch, Mock

from apilogs.bin import main
from conftest import event


def make_clients(get_resources_items=None, filter_events=None):
    logs_client = Mock()
    apigateway_client = Mock()
    apigateway_client.get_resources.return_value = {
        'items': get_resources_items or []}
    logs_client.filter_log_events.side_effect = (
        filter_events or (lambda **kwargs: {'events': []}))

    def make_client(name, **kwargs):
        return logs_client if name == 'logs' else apigateway_client

    return patch('boto3.client', side_effect=make_client), logs_client


def test_get_builds_gateway_group_from_api_id_and_stage():
    patcher, logs_client = make_clients()
    with patcher:
        code = main(['apilogs', 'get', '--api-id', 'xyz789', '--stage',
                     'test', '--no-color'])
    assert code == 0
    queried = [call.kwargs['logGroupName']
               for call in logs_client.filter_log_events.call_args_list]
    assert queried[0] == 'API-Gateway-Execution-Logs_xyz789/test'


def test_get_prints_gateway_events_with_group_and_stream(capsys):
    patcher, logs_client = make_clients(filter_events=lambda **kwargs: {
        'events': [event('1', 1, 'hello from cli', 'gw-stream')]})
    with patcher:
        code = main(['apilogs', 'get', '--api-id', 'xyz789', '--stage',
                     'test', '--no-color'])
    assert code == 0
    assert capsys.readouterr().out == (
        'API-Gateway-Execution-Logs_xyz789/test gw-stream '
        ' hello from cli\n')


def test_get_resolves_relative_start_time():
    patcher, logs_client = make_clients()
    with patcher:
        code = main(['apilogs', 'get', '--api-id', 'xyz789', '--stage',
                     'test', '-s', '2h', '--no-color'])
    assert code == 0
    assert logs_client.filter_log_events.call_args_list[0].kwargs[
        'startTime'] > 0


def test_unknown_date_returns_code_3(capsys):
    code = main(['apilogs', 'get', '--api-id', 'xyz789', '--stage', 'test',
                 '-s', 'nonsense', '--no-color'])
    assert code == 3
    assert "doesn't understand 'nonsense' as a date" in capsys.readouterr().err


def test_version_exits_zero():
    with pytest.raises(SystemExit) as excinfo:
        main(['apilogs', '--version'])
    assert excinfo.value.code == 0


def test_help_exits_zero():
    with pytest.raises(SystemExit) as excinfo:
        main(['apilogs', '--help'])
    assert excinfo.value.code == 0


def test_get_short_flags_gs(capsys):
    patcher, _ = make_clients(filter_events=lambda **kwargs: {
        'events': [event('1', 1, 'bare message', 'gw-stream')]})
    with patcher:
        code = main(['apilogs', 'get', '-GS', '--api-id', 'xyz789',
                     '--stage', 'test', '--no-color'])
    assert code == 0
    assert capsys.readouterr().out == 'bare message\n'
