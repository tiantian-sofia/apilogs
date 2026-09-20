"""Tests for ``AWSLogs.get_lambda_function_names``.

Covers Lambda integration-URI parsing (including the ``:name:alias``
suffix), skipping of non-Lambda integration types, and resources that do
not expose ``resourceMethods``.
"""
from apilogs import AWSLogs
from conftest import API_ID, STAGE

LAMBDA_URI_TEMPLATE = (
    'arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/'
    'arn:aws:lambda:us-east-1:123456789012:function:{name}/invocations'
)


def lambda_uri(name):
    return LAMBDA_URI_TEMPLATE.format(name=name)


def make_resource(resource_id, methods):
    resource = {'id': resource_id}
    if methods is not None:
        resource['resourceMethods'] = methods
    return resource


def configure(apigateway_client, resources, integrations):
    """Wire get_resources / get_integration mocks.

    ``integrations`` maps ``(resource_id, http_method)`` to an integration
    dict; methods without an entry are not expected to be queried.
    """
    apigateway_client.get_resources.return_value = {'items': resources}

    def get_integration(restApiId, resourceId, httpMethod):
        return integrations[(resourceId, httpMethod)]

    apigateway_client.get_integration.side_effect = get_integration


def test_extracts_plain_function_name(clients):
    configure(clients['apigateway'],
              [make_resource('r1', {'GET': {}})],
              {('r1', 'GET'): {'type': 'AWS_PROXY',
                               'uri': lambda_uri('hello-world')}})
    awslogs = AWSLogs()
    assert awslogs.get_lambda_function_names(API_ID, STAGE) == ['hello-world']
    clients['apigateway'].get_resources.assert_called_once_with(
        restApiId=API_ID)


def test_keeps_alias_suffix_in_extracted_name(clients):
    # The integration URI is functions/...:function:name:PROD/invocations.
    configure(clients['apigateway'],
              [make_resource('r1', {'GET': {}})],
              {('r1', 'GET'): {'type': 'AWS_PROXY',
                               'uri': lambda_uri('hello-world:PROD')}})
    awslogs = AWSLogs()
    assert awslogs.get_lambda_function_names(API_ID, STAGE) == [
        'hello-world:PROD']


def test_alias_suffix_is_stripped_when_building_log_group(clients):
    # list_logs turns "name:alias" into the base log group "/aws/lambda/name"
    # (Lambda alias invocations still write to the function's base group).
    configure(clients['apigateway'],
              [make_resource('r1', {'GET': {}})],
              {('r1', 'GET'): {'type': 'AWS_PROXY',
                               'uri': lambda_uri('hello-world:PROD')}})
    from conftest import GATEWAY_GROUP
    awslogs = AWSLogs(api_id=API_ID, stage=STAGE,
                      log_group_name=GATEWAY_GROUP, log_stream_name='ALL',
                      color_enabled=False, output_stream_enabled=True,
                      output_group_enabled=True)
    awslogs.list_logs()
    queried = [call.kwargs['logGroupName']
               for call in clients['logs'].filter_log_events.call_args_list]
    assert '/aws/lambda/hello-world' in queried
    assert '/aws/lambda/hello-world:PROD' not in queried


def test_aws_type_with_lambda_uri_is_included(clients):
    configure(clients['apigateway'],
              [make_resource('r1', {'POST': {}})],
              {('r1', 'POST'): {'type': 'AWS',
                                'uri': lambda_uri('classic-aws')}})
    awslogs = AWSLogs()
    assert awslogs.get_lambda_function_names(API_ID, STAGE) == ['classic-aws']


def test_skips_non_lambda_integration_types(clients):
    configure(clients['apigateway'],
              [make_resource('r1', {'GET': {}, 'POST': {}, 'DELETE': {}})],
              {
                  ('r1', 'GET'): {'type': 'HTTP', 'uri': 'https://example.com'},
                  ('r1', 'POST'): {'type': 'MOCK'},
                  ('r1', 'DELETE'): {'type': 'AWS_PROXY',
                                     'uri': lambda_uri('real-fxn')},
              })
    awslogs = AWSLogs()
    assert awslogs.get_lambda_function_names(API_ID, STAGE) == ['real-fxn']


def test_skips_lambda_typed_integration_without_function_uri(clients):
    # An AWS integration pointing at a non-Lambda service must be skipped.
    configure(clients['apigateway'],
              [make_resource('r1', {'GET': {}, 'POST': {}})],
              {
                  ('r1', 'GET'): {
                      'type': 'AWS',
                      'uri': ('arn:aws:apigateway:us-east-1:s3:path/'
                              'my-bucket/key')},
                  ('r1', 'POST'): {'type': 'AWS_PROXY',
                                   'uri': lambda_uri('real-fxn')},
              })
    awslogs = AWSLogs()
    assert awslogs.get_lambda_function_names(API_ID, STAGE) == ['real-fxn']


def test_resource_without_resource_methods_is_ignored(clients):
    configure(clients['apigateway'],
              [make_resource('r-parent', None),
               make_resource('r-child', {'GET': {}})],
              {('r-child', 'GET'): {'type': 'AWS_PROXY',
                                    'uri': lambda_uri('child-fxn')}})
    awslogs = AWSLogs()
    assert awslogs.get_lambda_function_names(API_ID, STAGE) == ['child-fxn']
    # get_integration must only be called for the method-bearing resource.
    assert [call.kwargs['resourceId']
            for call in clients['apigateway'].get_integration.call_args_list] \
        == ['r-child']


def test_multiple_methods_and_resources_are_all_collected(clients):
    configure(clients['apigateway'],
              [make_resource('r1', {'GET': {}, 'POST': {}}),
               make_resource('r2', {'PUT': {}})],
              {
                  ('r1', 'GET'): {'type': 'AWS_PROXY',
                                  'uri': lambda_uri('fxn-a')},
                  ('r1', 'POST'): {'type': 'AWS_PROXY',
                                   'uri': lambda_uri('fxn-b')},
                  ('r2', 'PUT'): {'type': 'AWS_PROXY',
                                  'uri': lambda_uri('fxn-c')},
              })
    awslogs = AWSLogs()
    assert awslogs.get_lambda_function_names(API_ID, STAGE) == [
        'fxn-a', 'fxn-b', 'fxn-c']


def test_no_resources_returns_empty_list(clients):
    configure(clients['apigateway'], [], {})
    awslogs = AWSLogs()
    assert awslogs.get_lambda_function_names(API_ID, STAGE) == []
    assert clients['apigateway'].get_integration.call_count == 0
