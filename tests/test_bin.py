from unittest.mock import patch

import pytest

from apilogs.bin import main


@pytest.fixture
def apilogs_cls():
    with patch('apilogs.bin.AWSLogs') as cls:
        yield cls


def test_get_builds_log_group_name_from_api_id_and_stage(apilogs_cls):
    assert main(['apilogs', 'get', '--api-id', 'abc123', '--stage', 'prod',
                 '--no-color']) == 0
    kwargs = apilogs_cls.call_args[1]
    assert kwargs['api_id'] == 'abc123'
    assert kwargs['stage'] == 'prod'
    assert kwargs['log_group_name'] == 'API-Gateway-Execution-Logs_abc123/prod'
    assert kwargs['log_stream_name'] == 'ALL'
    apilogs_cls.return_value.list_logs.assert_called_once_with()


def test_get_short_flags(apilogs_cls):
    assert main(['apilogs', 'get', '-a', 'abc123', '-t', 'dev']) == 0
    kwargs = apilogs_cls.call_args[1]
    assert kwargs['log_group_name'] == 'API-Gateway-Execution-Logs_abc123/dev'


def test_get_forwards_output_options(apilogs_cls):
    assert main(['apilogs', 'get', '-a', 'abc123', '-t', 'prod',
                 '--timestamp', '--ingestion-time', '-G', '-S',
                 '-H', 'ERROR', '-H', 'timeout', '--no-color',
                 '-s', '1h', '-e', '5m', '-f', '{$.level="error"}']) == 0
    kwargs = apilogs_cls.call_args[1]
    assert kwargs['output_timestamp_enabled'] is True
    assert kwargs['output_ingestion_time_enabled'] is True
    assert kwargs['output_group_enabled'] is False
    assert kwargs['output_stream_enabled'] is False
    assert kwargs['highlight'] == ['ERROR', 'timeout']
    assert kwargs['color_enabled'] is False
    assert kwargs['start'] == '1h'
    assert kwargs['end'] == '5m'
    assert kwargs['filter_pattern'] == '{$.level="error"}'


def test_no_subcommand_prints_help_and_returns_1(apilogs_cls, capsys):
    assert main(['apilogs']) == 1
    assert 'usage' in capsys.readouterr().out.lower()
    apilogs_cls.return_value.list_logs.assert_not_called()


def test_groups_dispatches_list_groups(apilogs_cls):
    assert main(['apilogs', 'groups']) == 0
    apilogs_cls.return_value.list_groups.assert_called_once_with()


def test_streams_dispatches_list_streams(apilogs_cls):
    assert main(['apilogs', 'streams', 'my-log-group']) == 0
    assert apilogs_cls.call_args[1]['log_group_name'] == 'my-log-group'
    apilogs_cls.return_value.list_streams.assert_called_once_with()


def test_version(capsys):
    from apilogs import __version__
    with pytest.raises(SystemExit) as excinfo:
        main(['apilogs', '--version'])
    assert excinfo.value.code == 0
    assert __version__ in capsys.readouterr().out
