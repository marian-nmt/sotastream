# this pipeline is created to test loading of custom/private pipelines from a runtime directory

from sotastream.pipelines import Pipeline, pipeline
from sotastream.pipelines.default import DefaultPipeline

import logging as log

log.basicConfig(level=log.INFO)


@pipeline('dummy')
class DummyPipeline(DefaultPipeline):
    def __init__(self, path, **kwargs):
        super().__init__(path, **kwargs)
        self.myarg = kwargs['myarg']
        log.info(f'Loaded sample pipeline with myarg={self.myarg}')

    @classmethod
    def add_cli_args(cls, parser):
        super().add_cli_args(parser)
        parser.add_argument('--myarg', required=True, help='Sample pipeline argument (required)')


@pipeline('dummy2')
class DummyPipeline2(DefaultPipeline):
    def __init__(self, path, **kwargs):
        super().__init__(path, **kwargs)
        self.myarg2 = kwargs['myarg2']
        log.info(f'Loaded sample pipeline with myarg={self.myarg2}')

    @classmethod
    def add_cli_args(cls, parser):
        super().add_cli_args(parser)
        parser.add_argument('--myarg2', required=True, help='Sample pipeline argument (required)')
