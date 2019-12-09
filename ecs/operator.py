# -*- coding: utf-8 -*-
# Copyright 2019 Royal FloraHolland
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import datetime as dt
import logging

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.operators.ecs_operator import ECSOperator
from botocore.exceptions import WaiterError, ClientError


class CustomECSOperator(ECSOperator):

    def __init__(self,
                 task_definition,
                 cluster,
                 overrides,
                 aws_conn_id=None,
                 region_name=None,
                 log_group=None,
                 log_prefix=None,
                 **kwargs):
        super(CustomECSOperator, self).__init__(
            task_definition=task_definition,
            cluster=cluster,
            overrides=overrides,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
            **kwargs
        )
        self.log_group = log_group
        self.log_prefix = log_prefix

    def _wait_for_task_ended(self):
        hook = AwsHook()
        ecs_client = hook.get_client_type('ecs', region_name='eu-west-1')
        task = ecs_client.describe_tasks(cluster='cluster', tasks=[self.arn])['tasks'][0]
        logging.warn(f"TASK: {task}")
        container_name = task['containers'][0]['name']

        log_client = hook.get_client_type('logs', region_name='eu-west-1')
        log_stream_name = f"{self.log_prefix}/{container_name}/{self.arn.split('/')[1]}"

        # hackish waiting pattern - use exception for control flow:
        # - let waiter poll for task stop
        # - return on success, repeat when wait fails
        #
        # while looping:
        # - log events on each try and after waiter succeeds
        #
        # this should give us all events that have been logged by the task
        token = None
        while True:
            token = self._log_events(log_client, log_stream_name, token)

            waiter = self.client.get_waiter('tasks_stopped')
            waiter.config.max_attempts = 2
            try:
                waiter.wait(
                    cluster=self.cluster,
                    tasks=[self.arn]
                )
                self._log_events(log_client, log_stream_name, token)
                return
            except WaiterError:
                continue

    def _log_events(self, client, log_stream_name, token=None):
        logging.info("Retrieving events from {}".format(log_stream_name))
        try:
            if token:
                response = client.get_log_events(
                    logGroupName=self.log_group,
                    logStreamName=log_stream_name,
                    nextToken=token
                )
            else:
                response = client.get_log_events(
                    logGroupName=self.log_group,
                    logStreamName=log_stream_name,
                    startFromHead=True
                )
            logging.info("Retrieved {} events:".format(len(response['events'])))
            token = response['nextForwardToken']

            for event in response['events']:
                ts_seconds = int(event['timestamp']) / 1000
                datetime = dt.datetime.fromtimestamp(ts_seconds)
                logging.info("{}: {}".format(datetime, event['message']))
        except ClientError as e:
            logging.warn("Unable to read events: {}".format(e))

        return token
