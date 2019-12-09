import copy
import datetime as dt
import json
import os

from airflow import DAG

from ecs import task_factory
from ecs.operator import CustomECSOperator
from ecs.task_reader import fetch_definitions

CACHE = '/tmp/scan_ecs_dags.json'


def create_task(task_name, config, arn, container):
    if 'class' not in config:
        args = config.get('args', [])
        command = config.get('command', task_name)
        return CustomECSOperator(
            task_id=task_name,
            task_definition=arn,
            overrides={
                'containerOverrides': [{
                    'name': container['name'],
                    'command': [command, *args]
                }]
            },
            cluster='cluster',
            region_name='eu-west-1',
            log_group=container['logConfiguration']['options']['awslogs-group'],
            log_prefix=container['logConfiguration']['options']['awslogs-stream-prefix']
        )
    else:
        kwargs = config.get('kwargs', {})
        return task_factory.create_task(task_name, config['class'], **kwargs)


def scan_tasks(registry):
    mtime = None
    if os.path.exists(CACHE):
        mtime = os.path.getmtime(CACHE)

    if mtime is not None and dt.datetime.now().timestamp() - mtime < 300:
        with open(CACHE, 'r') as cache:
            definitions = json.load(cache)
    else:
        definitions = fetch_definitions()
        with open(CACHE, 'w') as cache:
            cache.write(json.dumps(definitions))

    for name, labels in definitions.items():
        print(f'Registering ECS DAG with name "{name}", with labels {labels["airflow"]}')
        tasks = labels['airflow']['tasks']
        args = copy.deepcopy(labels['airflow']['dag'])
        if 'start_date' in args:
            args['start_date'] = dt.datetime.strptime(args['start_date'], "%Y-%m-%dT%H:%M:%S")
        dag_kwargs = {}
        if 'concurrency' in args:
            dag_kwargs['concurrency'] = int(args['concurrency'])
        if 'schedule_interval' in args:
            dag_kwargs['schedule_interval'] = args['schedule_interval']

        with DAG(name, default_args=args, **dag_kwargs) as dag:
            registry[name] = dag

            ops = {}
            # loop twice to be able to link tasks
            for task_name, task_def in tasks.items():
                task = create_task(task_name, task_def, labels['arn'], labels['container'])
                ops[task_name] = task
            for task_name, task_def in tasks.items():
                for depend in task_def.get('depends', []):
                    ops[task_name].set_upstream(ops[depend])

scan_tasks(globals())
