import logging
from typing import Callable
from functools import partial

from sotastream.augmentors import *
from sotastream import Defaults
from sotastream.filters import BitextFilter

from . import DocumentPipeline, pipeline

logger = logging.getLogger(f"sotastream")


@pipeline("example")
class ExamplePipeline(DocumentPipeline):
    description = "Example pipeline with two data streams"

    def __init__(self, parallel_data, backtrans_data, **kwargs):
        super().__init__(**kwargs)

        parallel = self.create_data_stream(parallel_data, processor=ReadAndAugment)
        backtrans = self.create_data_stream(backtrans_data, processor=partial(ReadAndAugment, tag="<FR>"))

        stream = Mixer([parallel, backtrans], self.mix_weights)
        self.stream = BitextFilter(stream)  # removes all but fields 0 and 1

    @classmethod
    def get_data_sources_for_argparse(cls):
        return [
            ('parallel_data', 'Path to parallel data (folder with .gz files, or compressed TSV)'),
            ('backtrans_data', 'Path to backtranslation data (folder with .gz files, or compressed TSV)'),
        ]

    @classmethod
    def get_data_sources_default_weights(cls):
        return [0.5, 0.5]


def LowerCase(stream):
    for line in stream:
        line[0] = line[0].lower()
        yield line


def TitleCase(stream):
    for line in stream:
        line[0] = line[0].title()
        line[1] = line[1].title()
        yield line


def TagData(stream, tag):
    for line in stream:
        line[0] = f"{tag} {line}"
        yield line


def ReadAndAugment(path: str, tag: str = None):
    """
    Opens a file as a stream and passes it through an augmentor.
    """
    stream = UTF8File(path)

    # Randomly mix in case
    stream = Mixer(
        [
            stream,
            LowerCase(stream),
            TitleCase(stream),
        ],
        [0.95, 0.04, 0.01],
    )

    if tag is not None:
        stream = TagData(stream, tag)

    return stream
