import os
import sys

sys.dont_write_bytecode = True


def get_version():
    package_dir = os.path.dirname(__file__)
    root_dir = os.path.abspath(os.path.join(package_dir, '..'))
    pyproject_path = os.path.join(root_dir, 'pyproject.toml')

    if os.path.exists(pyproject_path):
        with open(pyproject_path) as fh:
            for line in fh:
                if line.startswith("version ="):
                    return line.split('"')[1].strip()

    return None


__version__ = get_version()


class Defaults:
    """
    Default values for pipeline arguments
    """

    BUFFER_SIZE = 1_000_000
    QUEUE_BUFFER_SIZE = 10_000
    SEPARATOR = " "
    DOC_SEPARATOR = " <eos>"
    SAMPLE_FILE = None
    SPM_MODEL = None
    SEED = 0
    MAX_TOKENS = 250
    SAMPLE_LENGTH = True
    QUIET = False
    NUM_PROCESSES = 16
    DOC_PROB = 0.0
    DOC_PROB_PARALLEL = 0.0
    SHUFFLE = True


from .filters import *

# BUG: note that this will result in import order bug: .augmentors.{doc,robustness}.* won't be available under .augmentors
# from .augmentors import *
from . import augmentors
from .data import Line
from .utils import *
from .cli import main as cli_main
