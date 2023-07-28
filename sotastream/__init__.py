import sys

sys.dont_write_bytecode = True

from importlib import metadata

__version__ = metadata.version(__package__)


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
