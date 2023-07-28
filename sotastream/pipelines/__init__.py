import importlib
import os
import sys
import logging as logger

from pathlib import Path

from .base import Pipeline, DocumentPipeline

logger.basicConfig(level=logger.INFO)
PIPELINES = {}  # dict of pipeline name -> pipeline class


def pipeline(name: str):
    """
    Register a pipeline class to the PIPELINES dict.
    The name is used to index the class object, e.g., from the command line.

    :param name: name of component i.e., pipeline name e.g, "t1"
    :return: a decorator.
    """
    assert name not in PIPELINES, f"Pipeline {name} already taken by {PIPELINES[name]}"

    def decorator(cls):
        PIPELINES[name] = cls
        return cls

    return decorator


"""
Load all modules in the current directory matching the pattern "*_pipeline.py".
"""
modules = Path(__file__).parent.glob("*.py")
__all__ = [f.name.replace('.py', '') for f in modules if f.is_file() and not f.name.startswith('__')]

FAIL_ON_ERROR = os.environ.get('SOTASTREAM_FAIL_ON_ERROR', False)

for module_name in __all__:
    try:
        importlib.import_module(f'.{module_name}', __package__)
    except Exception as ex:
        logger.error(f'Unable to load {module_name}: {ex}')
        if FAIL_ON_ERROR:
            raise ex

for path in list(Path(os.getcwd()).glob('*_pipeline.py')):
    module_name = path.name.replace('.py', '')
    if module_name in sys.modules:
        raise Exception(
            f'Module name {module_name} from {path} collides with an already imported module.\
            This state might lead to hard-to-find bugs. Please rename your module.'
        )
    try:
        importlib.import_module(module_name, __package__)
    except:
        logger.error(
            f'Error while importing {path}. \
            Double check that you have installed all the required libraries.'
        )
        raise
