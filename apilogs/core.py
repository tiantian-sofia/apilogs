import logging
import re
import sys
import os
import time
from threading import Thread, Event
from datetime import datetime, timedelta
from collections import deque
try:
    from Queue import Queue
except ImportError:
    from queue import Queue

import boto3
from botocore.compat import total_seconds

from termcolor import colored
from dateutil.parser import parse

from . import exceptions
from operator import itemgetter, attrgetter, methodcaller

def milis2iso(milis):
    res = datetime.utcfromtimestamp(milis/1000.0).isoformat()
    return (res + ".000")[:23] + 'Z'

log = logging.getLogger(__name__)

class AWSLogs(object):

    ACTIVE = 1
    EXHAUSTED = 2
    WATCH_SLEEP = 2

    FILTER_LOG_EVENTS_STREAMS_LIMIT = 300
    MAX_EVENTS_PER_CALL = 10000
    ALL_WILDCARD = 'ALL'
    LAMBDA_NAMES_CACHE_SECONDS = 300

    def __init__(self, **kwargs):
        self.aws_region = kwargs.get('aws_region')
        self.aws_access_key_id = kwargs.get('aws_access_key_id')
        self.aws_secret_access_key = kwargs.get('aws_secret_access_key')
        self.aws_session_token = kwargs.get('aws_session_token')
        self.log_group_name = kwargs.get('log_group_name')
        self.api_id = kwargs.get('api_id')
        self.stage = kwargs.get('stage')
        self.log_stream_name = kwargs.get('log_stream_name')
        self.filter_pattern = kwargs.get('filter_pattern')
        self.highlight = kwargs.get('highlight')
        self.watch = kwargs.get('watch')
        self.color_enabled = kwargs.get('color_enabled')
        self.output_stream_enabled = kwargs.get('output_stream_enabled')
        self.output_group_enabled = kwargs.get('output_group_enabled')
        self.output_timestamp_enabled = kwargs.get('output_timestamp_enabled')
        self.output_ingestion_time_enabled = kwargs.get(
            'output_ingestion_time_enabled')
        self.start = self.parse_datetime(kwargs.get('start'))
        self.end = self.parse_datetime(kwargs.get('end'))
        self.next_tokens = {}
        self._lambda_function_names = None
        self._lambda_function_names_fetched_at = 0

        self.client = boto3.client(
            'logs',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_session_token=self.aws_session_token,
            region_name=self.aws_region
        )

        self.apigClient = boto3.client(
            'apigateway',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_session_token=self.aws_session_token,
            region_name=self.aws_region
        )

    def _get_streams_from_pattern(self, group, pattern):
        """Returns streams in ``group`` matching ``pattern``."""
        pattern = '.*' if pattern == self.ALL_WILDCARD else pattern
        reg = re.compile('^{0}'.format(pattern))

        # print pattern
        for stream in self.get_streams(group):
            if re.match(reg, stream):
                yield stream

    def get_lambda_function_names(self, apiId, stage):
        # todo: get functions from actual deployment. SDK needs to support embed=apisummary parameter
        # stage = self.apigClient.get_stage(restApiId=apiId, stageName=stage)
        # dep_id = stage['deploymentId']
        # dep = self.apigClient.get_deployment(restApiId=apiId, deploymentId=dep_id)
        # print dep['apiSummary']

        names = []
        resources = self.apigClient.get_resources(restApiId=apiId)['items']

        # note: this currently returns the lambda functions from the head revision, which may be different than the deployed version
        for resource in resources:
            if 'resourceMethods' in resource:
                methods = resource['resourceMethods']
                for method in methods:
                    integ = self.apigClient.get_integration(restApiId=apiId,
                                                    resourceId=resource['id'],
                                                    httpMethod=method)
                    if (integ['type'] == "AWS" or integ['type'] == "AWS_PROXY") and "lambda:path/2015-03-31/functions" in integ['uri']:
                        uri = integ['uri']
                        start = uri.find(":function:")
                        end = uri.find("/invocations")
                        name = uri[start + 10:end]
                        names.append(name)
        return names

    def _get_lambda_function_names(self):
        """Returns Lambda function names, cached for a few minutes so that
        watch mode does not hit the API Gateway control plane on every poll.
        """
        now = time.time()
        if (self._lambda_function_names is None or
                now - self._lambda_function_names_fetched_at >
                self.LAMBDA_NAMES_CACHE_SECONDS):
            self._lambda_function_names = self.get_lambda_function_names(
                self.api_id, self.stage)
            self._lambda_function_names_fetched_at = now
        return self._lambda_function_names

    def list_logs(self):
        streams = []

        if self.log_stream_name != self.ALL_WILDCARD:
            streams = list(self._get_streams_from_pattern(self.log_group_name, self.log_stream_name))

            if len(streams) > self.FILTER_LOG_EVENTS_STREAMS_LIMIT:
                raise exceptions.TooManyStreamsFilteredError(
                     self.log_stream_name,
                     len(streams),
                     self.FILTER_LOG_EVENTS_STREAMS_LIMIT
                )
            if len(streams) == 0:
                raise exceptions.NoStreamsFilteredError(self.log_stream_name)

        max_stream_length = max([len(s) for s in streams]) if streams else 10
        group_length = len(self.log_group_name)

        queue, exit = Queue(), Event()

        def update_next_token(response, group):
            if 'nextToken' in response:
                self.next_tokens[group] = response['nextToken']
            else:
                self.next_tokens.pop(group, None)

        def list_lambda_logs(allevents, base_kwargs):
            # add events from lambda function streams
            fxns = self._get_lambda_function_names()
            for fxn in fxns:
                lambda_group = ("/aws/lambda/" + fxn).split(':')[0]
                call_kwargs = dict(base_kwargs, logGroupName=lambda_group)
                token = self.next_tokens.get(lambda_group)
                if token:
                    call_kwargs['nextToken'] = token
                try:
                    lambda_response = filter_log_events(**call_kwargs)
                    events = lambda_response.get('events', [])
                    for event in events:
                        event['group_name'] = lambda_group
                        allevents.append(event)
                    update_next_token(lambda_response, lambda_group)
                except Exception as e:
                    # If the log group is gone (e.g. the function was never
                    # invoked), drop any stale token so it cannot block the
                    # exit condition forever.
                    error_code = getattr(e, 'response', {}).get(
                        'Error', {}).get('Code', '')
                    if error_code == 'ResourceNotFoundException':
                        self.next_tokens.pop(lambda_group, None)
                    log.warning("Error fetching logs for Lambda function {0}"
                                " with group {1}. This function may need to be"
                                " invoked.".format(fxn, lambda_group))
            return allevents

        def list_apigateway_logs(allevents, base_kwargs):
            # add events from API Gateway streams
            call_kwargs = dict(base_kwargs, logGroupName=self.log_group_name)
            token = self.next_tokens.get(self.log_group_name)
            if token:
                call_kwargs['nextToken'] = token

            try:
                apigresponse = filter_log_events(**call_kwargs)
            except Exception as e:
                log.error(
                    "Error fetching logs for API {0}. Please ensure logging "
                    "is enabled for this API and the API is deployed. See "
                    "http://docs.aws.amazon.com/apigateway/latest/"
                    "developerguide/how-to-stage-settings.html: {1}"
                        .format(self.api_id, e))
                raise

            events = apigresponse.get('events', [])
            for event in events:
                event['group_name'] = self.log_group_name
                allevents.append(event)
            update_next_token(apigresponse, self.log_group_name)
            return allevents

        def filter_log_events(**kwargs):
            try:
                resp = self.client.filter_log_events(**kwargs)

                if 'nextToken' in resp:
                    group = kwargs['logGroupName']
                    next = resp['nextToken']
                    #print "Resp: Group: " + group + " nextToken: " + next

                #print resp

                return resp
            except Exception as e:
                log.error("Caught error from CloudWatch: {0}".format(e))
                raise


        def consumer():
            while not exit.is_set():
                event = queue.get()

                if event is None:
                    exit.set()
                    break

                # Strip any tail line feeds
                message = event['message'].rstrip("\r\n")
                if self.highlight:
                    for value in self.highlight:
                        if value and not value.isspace():
                            message = message.replace(value, self.color(value, 'blue', 'on_yellow'))
                output = []
                if self.output_group_enabled:
                    output.append(
                        self.color(
                            event['group_name'].ljust(group_length, ' '),
                            'green'
                        )
                    )
                if self.output_stream_enabled:
                    output.append(
                        self.color(
                            event['logStreamName'].ljust(max_stream_length,
                                                         ' '),
                            'cyan'
                        )
                    )
                if self.output_timestamp_enabled:
                    output.append(
                        self.color(
                            milis2iso(event['timestamp']),
                            'yellow'
                        )
                    )
                if self.output_ingestion_time_enabled:
                    output.append(
                        self.color(
                            milis2iso(event['ingestionTime']),
                            'blue'
                        )
                    )

                output.append(message)
                print(' '.join(output))
                sys.stdout.flush()

        def generator():
            """Push events into queue trying to deduplicate them using a lru queue.
            AWS API stands for the interleaved parameter that:
                interleaved (boolean) -- If provided, the API will make a best
                effort to provide responses that contain events from multiple
                log streams within the log group interleaved in a single
                response. That makes some responses return some subsequent
                response duplicate events. In a similar way when awslogs is
                called with --watch option, we need to findout which events we
                have alredy put in the queue in order to not do it several
                times while waiting for new ones and reusing the same
                next_token. The site of this queue is MAX_EVENTS_PER_CALL in
                order to not exhaust the memory.
            """
            interleaving_sanity = deque(maxlen=self.MAX_EVENTS_PER_CALL)
            kwargs = {'interleaved': True}

            if streams:
                kwargs['logStreamNames'] = streams

            if self.start:
                kwargs['startTime'] = self.start

            if self.end:
                kwargs['endTime'] = self.end

            if self.filter_pattern:
                kwargs['filterPattern'] = self.filter_pattern

            try:
                while not exit.is_set():
                    allevents = []

                    list_apigateway_logs(allevents, kwargs)
                    list_lambda_logs(allevents, kwargs)

                    allevents.sort(key=itemgetter('timestamp'))

                    for event in allevents:
                        if event['eventId'] not in interleaving_sanity:
                            interleaving_sanity.append(event['eventId'])
                            queue.put(event)

                    # Send the exit signal if no more pages and not in watch mode
                    if not self.watch and not self.next_tokens:
                        break

                    if self.watch:
                        exit.wait(self.WATCH_SLEEP)
            finally:
                # Always signal the consumer, even on unexpected errors,
                # so non-watch mode can never hang waiting for events.
                queue.put(None)

        g = Thread(target=generator)
        g.start()

        c = Thread(target=consumer)
        c.start()

        try:
            while not exit.is_set():
                time.sleep(.1)
        except (KeyboardInterrupt, SystemExit):
            exit.set()
            print('Closing...\n')
            os._exit(0)

    def list_groups(self):
        """Lists available CloudWatch logs groups"""
        for group in self.get_groups():
            print(group)

    def list_streams(self):
        """Lists available CloudWatch logs streams in ``log_group_name``."""
        for stream in self.get_streams():
            print(stream)

    def get_groups(self):
        """Returns available CloudWatch logs groups"""
        paginator = self.client.get_paginator('describe_log_groups')
        for page in paginator.paginate():
            for group in page.get('logGroups', []):
                yield group['logGroupName']

    def get_streams(self, log_group_name=None):
        """Returns available CloudWatch logs streams in ``log_group_name``."""
        kwargs = {'logGroupName': log_group_name or self.log_group_name}
        window_start = self.start or 0
        window_end = self.end or sys.float_info.max

        paginator = self.client.get_paginator('describe_log_streams')
        for page in paginator.paginate(**kwargs):
            for stream in page.get('logStreams', []):
                if 'firstEventTimestamp' not in stream:
                    # This is a specified log stream rather than
                    # a filter on the whole log group, so there's
                    # no firstEventTimestamp.
                    yield stream['logStreamName']
                elif max(stream['firstEventTimestamp'], window_start) <= \
                        min(stream['lastEventTimestamp'], window_end):
                    yield stream['logStreamName']

    def color(self, text, color, on_color=None):
        """Returns coloured version of ``text`` if ``color_enabled``."""
        if self.color_enabled:
            return colored(text, color, on_color)
        return text

    def parse_datetime(self, datetime_text):
        """Parse ``datetime_text`` into a ``datetime``."""

        if not datetime_text:
            return None

        ago_regexp = r'(\d+)\s?(m|minute|minutes|h|hour|hours|d|day|days|w|weeks|weeks)(?: ago)?'
        ago_match = re.match(ago_regexp, datetime_text)

        if ago_match:
            amount, unit = ago_match.groups()
            amount = int(amount)
            unit = {'m': 60, 'h': 3600, 'd': 86400, 'w': 604800}[unit[0]]
            date = datetime.utcnow() + timedelta(seconds=unit * amount * -1)
        else:
            try:
                date = parse(datetime_text)
            except ValueError:
                raise exceptions.UnknownDateError(datetime_text)

        return int(total_seconds(date - datetime(1970, 1, 1))) * 1000
