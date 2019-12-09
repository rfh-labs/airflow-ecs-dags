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
import logging

from airflow.contrib.hooks.aws_hook import AwsHook

from ecs.label_parser import parse_labels


def fetch_definitions():
    definitions = {}

    hook = AwsHook()
    ecs = hook.get_client_type('ecs', region_name='eu-west-1')
    first = True
    next_token = None
    while first or next_token is not None:
        if first:
            first = False
            ecs_tasks = ecs.list_task_definitions()
        else:
            ecs_tasks = ecs.list_task_definitions(nextToken=next_token)
        next_token = ecs_tasks['nextToken'] if 'nextToken' in ecs_tasks else None

        for arn in ecs_tasks['taskDefinitionArns']:
            ecs_task = ecs.describe_task_definition(
                taskDefinition=arn
            )
            containers = ecs_task['taskDefinition']['containerDefinitions']
            if len(containers) != 1:
                continue

            container = containers[0]
            if 'dockerLabels' not in container:
                continue

            labels = parse_labels(container['dockerLabels'])

            if 'airflow' not in labels:
                continue

            try:
                name = labels['airflow']['dag']['name']

                definitions[name] = {
                    'airflow': labels['airflow'],
                    'arn': arn,
                    'container': container
                }

            except KeyError as e:
                logging.warning(f"Invalid configuration: {labels}", exc_info=e)

    return definitions
