from sotastream.augmentors import DataSource, UTF8File

from . import Pipeline, pipeline


@pipeline('default')
class DefaultPipeline(Pipeline):
    def __init__(self, parallel_data, **kwargs):
        super().__init__(**kwargs)

        self.stream = self.create_data_stream(parallel_data)

    @classmethod
    def get_data_sources_for_argparse(cls):
        return [('parallel_data', 'Path to parallel data (folder with .gz files, or compressed TSV)')]

    @classmethod
    def get_data_sources_default_weights(cls):
        return [1.0]
