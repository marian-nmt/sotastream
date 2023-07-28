from abc import ABC
import itertools
import logging
import os

from sotastream import Defaults
from sotastream.augmentors import DataSource, UTF8File
from sentencepiece import SentencePieceProcessor
from typing import List, Tuple, Callable

logger = logging.getLogger(f"sotastream")


class Pipeline(ABC):
    """Pipeline base class

    * To add a pipeline, extend this class and add @pipeline("pipeline_name") decorator
    * CLI arguments
        * To specify the data source names for argparse, override `get_data_sources_for_argparse` class method.
        * To specify default mixing weights for argparse, override `get_data_sources_default_weights` class method.
        * To specify any other CLI arguments override `add_cli_args` class method.
        * Don't forget the @classmethod decorator and cls as first argument.
        * The filename should match the pattern {name}_pipeline.py, where {name} is the name used in the decorator.
    * All CLI arguments are passed as arguments to constrctor.
    * Refer to default.py or t1_pipeline.py for example pipelines.
    """

    def __init__(self, **kwargs) -> None:
        # Store the data sources. The length will vary based on how many the subclass expects.
        self.data_sources = kwargs.get('data_sources', [])

        spm_file = kwargs.get("spm", None)
        if spm_file:
            self.spm_model = SentencePieceProcessor(model_file=spm_file)
        else:
            logger.warning("Creating pipeline without an SPM model")
            self.spm_model = None
        self.sample_file = kwargs.get("sample_file")
        self.buffer_size = kwargs.get("buffer_size", Defaults.BUFFER_SIZE)
        self.queue_buffer_size = kwargs.get("queue_buffer_size", Defaults.QUEUE_BUFFER_SIZE)
        self.is_quiet = kwargs.get("quiet", Defaults.QUIET)
        self.seed = kwargs.get("seed", Defaults.SEED)
        self.max_tokens = kwargs.get("max_tokens", Defaults.MAX_TOKENS)
        self.sample_length = kwargs.get("sample_length", Defaults.SAMPLE_LENGTH)
        self.separator = kwargs.get("separator", Defaults.SEPARATOR)
        self.shuffle = not kwargs.get("no_shuffle", not Defaults.SHUFFLE)

        # These are set in the environment of the caller when multiprocessing is enabled.
        # Each sub-process gets a distinct worker ID and knows the total number of workers.
        # These values are used to allocate the shards of a data source in a round-robin
        # fashion, such that each subprocess has 1/Nth of the total data, ensuring that
        # reading from them in round-robin fashion produces a permutation over the training
        # data. You can see that these values are used below in the create_data_stream()
        # function, a wrapper around the underlying infinibatch data structure.
        self.worker_id = int(os.environ.get("SOTASTREAM_WORKER_ID", "0"))
        self.num_workers = int(os.environ.get("SOTASTREAM_WORKER_COUNT", "1"))

        # sanity-check mix_weights and normalize
        self.mix_weights = kwargs.get("mix_weights", self.get_data_sources_default_weights())
        if any([w < 0 for w in self.mix_weights]):
            raise ValueError("Mix weights must be non-negative")
        self.mix_weights = [w / sum(self.mix_weights) for w in self.mix_weights]
        if len(self.data_sources) != len(self.mix_weights):
            raise Exception(
                f'Data sources does not match weights: {self.data_sources} != {self.mix_weights} '
            )

        # log a message with the mix weights and the data source names
        # e.g., INFO:sotastream:Using mix weights: 99.75% (parallel_data) 0.25% (garbage_data)
        mix_weight_message = "Using mix weights:\n\t" + "\n\t".join(
            f"{w*100:.5g}% : {path}) ({arg and arg[0] or ''})"
            for w, arg, path in itertools.zip_longest(
                self.mix_weights, self.get_data_sources_for_argparse(), self.data_sources
            )
        )
        logger.info(mix_weight_message)

        self.stream = None  # to be initialized in subclass

    @classmethod
    def add_cli_args(cls, parser):
        """
        Add CLI arguments to pipeline specific subparser.
        These arguments are shared across all pipelines and appear after the pipeline name in the CLI.
        For global args that appear before the pipeline name, see sotastream.cli.add_cli_args
        """

        # Validate the default weights provided in the pipeline
        data_sources = cls.get_data_sources_for_argparse()
        mix_weights = cls.get_data_sources_default_weights()
        if len(mix_weights) != len(data_sources):
            raise ValueError(
                f"Number of data sources ({len(data_sources)}) must match number of weights ({len(mix_weights)})"
            )

        for arg_spec in cls.get_data_sources_for_argparse():
            name, desc = arg_spec[:2]
            nargs = None
            if len(arg_spec) > 2:
                nargs = arg_spec[2]
            parser.add_argument(name, help=desc, nargs=nargs)

        parser.add_argument("--spm", help="SPM model (for more accurate length calculation")
        parser.add_argument(
            "--sample-length",
            action="store_true",
            help="Whether to fill each sample with the maximum tokens (default) or first sample a length (uniformly at random).",
        )
        parser.add_argument(
            "--separator",
            default=" ",
            help="String to use when joining sentences for data augmentation (default: '%(default)s').",
        )
        parser.add_argument(
            "--augment",
            action="store_true",
            help="Whether to add capitalization and target-copy augmentations",
        )
        parser.add_argument(
            "--max-joined-tokens",
            "--max-tokens",
            "-m",
            dest="max_tokens",
            type=int,
            default=Defaults.MAX_TOKENS,
            help="Maximum number of tokens to join",
        )

        if len(mix_weights) == 1 and mix_weights[0] in ('+', '*'):
            mix_weights_default = None
            mix_weights_nargs = mix_weights[0]
        else:
            mix_weights_default = mix_weights
            mix_weights_nargs = len(mix_weights)

        parser.add_argument(
            "--mix-weights",
            "-w",
            type=float,
            metavar="WEIGHT",
            nargs=mix_weights_nargs,  # validate the number of weights provided by the user
            default=mix_weights_default,
            help="Weights to use when mixing data sources (will be normalized if don't sum to 1.0) (default: %(default)s)",
        )

    def create_data_stream(
        self, data_path, processor: Callable = UTF8File, buffer_size: int = None, ext: str = ".gz"
    ):
        """
        Wrapper around data source creation to allow for easy overriding in subclasses.

        The worker ID and number of workers is passed to the DataSource class, which uses
        them to select the subset of shards this process will have access to.

        :param data_path: Path to data source
        :param processor: Augmentor processor function to apply to each chunk
        :param buffer_size: The buffer size to use
        :param ext: The extension of the data source
        """
        return DataSource(
            data_path,
            processChunk=processor,
            ext=ext,
            buffer_size=buffer_size or self.buffer_size,
            seed=self.seed,
            worker_id=self.worker_id,
            num_workers=self.num_workers,
        )

    @classmethod
    def get_data_sources_for_argparse(cls) -> List[Tuple[str, str]]:
        """
        This returns a list of (name, description) pairs for each data source.
        This is used to instantiate the argparse subcommand with named positional arguments.
        These are not the actual instantiated data paths; for that, each class has
        The function name is quite verbose in order to minimize confusion.

        Returns:
            List[Tuple]: List of (name, description)
        """
        return [
            (
                'data',
                'Path to data source (a pre-split folder containing .gz files, or a single compressed TSV)',
            )
        ]

    @classmethod
    def get_data_sources_default_weights(cls) -> List[float]:
        """
        A list of floats corresponding to the number of data sources and specifying the mixture weights among them.
        These will be provided to the argparse subcommand as the default values for the --mix-weights argument.
        To get the actual instantiated values, use self.mix_weights.
        The function is named in an overly explicit way to avoid confusion between these two sources.
        """
        return [1.0]

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.stream)

    @staticmethod
    def create(name: str, *args, **kwargs):
        """
        Create an instance of Pipeline for a given pipeline name
        """
        from . import PIPELINES

        assert name in PIPELINES, f'No pipeline with name {name} found'
        return PIPELINES[name](*args, **kwargs)


class DocumentPipeline(Pipeline):
    """
    Extends Pipeline base with document-level CLI args.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.doc_separator = kwargs.get("doc_separator", Defaults.DOC_SEPARATOR)
        self.doc_prob = kwargs.get("doc_prob", Defaults.DOC_PROB)
        self.doc_prob_parallel = kwargs.get("doc_prob_parallel", Defaults.DOC_PROB_PARALLEL)

    @classmethod
    def add_cli_args(cls, parser):
        """
        Add document-specific arguments.
        """
        super().add_cli_args(parser)

        parser.add_argument(
            "--doc-separator",
            default=Defaults.DOC_SEPARATOR,
            help="Sentence joiner token to use when building docs (default: '%(default)s').",
        )
        parser.add_argument(
            "--doc-prob-parallel",
            type=float,
            default=Defaults.DOC_PROB,
            help="Probability of creating a doc from parallel data",
        )
        parser.add_argument(
            "--doc-prob",
            type=float,
            default=Defaults.DOC_PROB_PARALLEL,
            help="Probability of creating a doc from backtrans data",
        )
