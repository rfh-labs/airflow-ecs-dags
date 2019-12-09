import importlib


def create_task(task_id, class_name, **kwargs):
    module_name = class_name[:class_name.rfind('.')]
    rel_class_name = class_name[class_name.rfind('.')+1:]
    mod = importlib.import_module(module_name)
    clazz = getattr(mod, rel_class_name)
    return clazz(task_id=task_id, **kwargs)
