import argparse
import functools
import logging
from pathlib import Path
from typing import List, Tuple

from sotastream.augmentors import DataSource, Mixer, UTF8File
from sotastream.pipelines import Pipeline, pipeline

logger = logging.getLogger(f"sotastream")


@pipeline("multistream")
class MultiStreamPipeline(Pipeline):
    """Pipeline for mixing multiple (or variable) number opf datasources.

    This pipeline takes one more more data paths and mixes them together as given by --mix-weights parameter (default: equal ratios i.e. balance the sources).
    Example usecase: classification task, where each data stream is per class (default mix ratio is to balance classes)
    """

    def __init__(self, paths: List[Path], ext: str, mix_weights: List = None, **kwargs):
        """Pipeline for mixing variable number of data sources.

        :param paths: paths of data files to mix.
        :param ext: extension of chunked files inside data files specified in paths
        :param mix_weights: weights of data files in mixing. Should be one weight per input path. If None, all data files are mixed with equal weights.
        :param **kwargs: see Pipeline class for more arguments
        """
        if mix_weights:
            if len(mix_weights) != len(paths):
                raise ValueError(
                    f'--mix-weights should have one weight per data source; Given {len(paths)} data sources but {len(mix_weights)} weight(s).'
                )
        else:
            mix_weights = [1.0] * len(paths)
        # data_sources has paths as a nested list, so we remove it and pass paths list itself
        kwargs.pop('data_sources', None)
        super().__init__(mix_weights=mix_weights, data_sources=paths, **kwargs)

        assert paths
        assert len(paths) == len(self.mix_weights)
        assert abs(1 - sum(self.mix_weights)) <= 1e-6, f'{self.mix_weights} = {sum(self.mix_weights)} != 1.0'

        TsvChunkReader = functools.partial(DataSource, ext=ext, buffer_size=self.buffer_size, seed=self.seed)
        logger.info('Mixing data from paths:\n * ' + '\n * '.join([str(path) for path in paths]))
        streams = [TsvChunkReader(path, processChunk=UTF8File) for path in paths]
        if len(paths) == 1:
            pipeline = streams[0]
        else:
            pipeline = Mixer(streams, self.mix_weights)
        self.stream = pipeline

    @classmethod
    def get_data_sources_for_argparse(cls) -> List[Tuple]:
        return [
            (
                'paths',
                '''Dataset paths (i.e. sub datasets) to mix. Mixture weights can be specified with --mix-weights,
 one per path and in the same order as paths (Default: equal ratios).
 Each path should be a directory with chunked files ending with suffix given by --ext argument.''',
                '+',
            ),
        ]

    @classmethod
    def get_data_sources_default_weights(cls):
        # we dont know how many sources will be provided until runtime CLI parsing
        return ['+']

    @classmethod
    def add_cli_args(cls, parser: argparse.ArgumentParser):
        super().add_cli_args(parser)
        parser.add_argument(
            '--ext',
            '-e',
            type=str,
            default='.tsv',
            help='Extensions of chunked files inside data directories.\n Default: .tsv. '
            'For gzip compressed files set .gz',
        )
