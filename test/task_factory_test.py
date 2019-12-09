from ecs import task_factory


def test_task_factory():
    kwargs = {'a': 'b'}
    task = task_factory.create_task(
        'ye-task',
        'ecstest.dynamic_import.Dynamic',
        **kwargs
    )
    assert task.task_id == 'ye-task'
    assert task.kwargs == {'a': 'b'}


def test_task_factory_without_args():
    task = task_factory.create_task(
        'ye-task',
        'ecstest.dynamic_import.Dynamic'
    )
    assert task.task_id == 'ye-task'
