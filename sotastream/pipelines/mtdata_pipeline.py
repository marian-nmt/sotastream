import logging
from typing import Tuple, Iterator, Union, List, Optional
import random

from sotastream.data import Line
from sotastream.augmentors import Mixer
from sotastream.filters import BitextFilter
from sotastream.pipelines import Pipeline, pipeline

logger = logging.getLogger(f"sotastream")


@pipeline("mtdata")
class MTDataPipeline(Pipeline):
    """Pipeline to mix datasets from mtdata.

    To install mtdata, run `pip install mtdata`, or visit https://github.com/thammegowda/mtdata
    To see the list of available datasets, run `mtdata list -id -l <src>-<tgt>` where <src>-<tgt>
    are language pairs.

    Example #1:
        sotastream mtdata -lp en-de Statmt-news_commentary-16-deu-eng  Statmt-europarl-10-deu-eng

    Example #2:
        sotastream mtdata -lp en-de Statmt-news_commentary-16-deu-eng  Statmt-europarl-10-deu-eng --mix-weights 1 2

    Example #3:
        sotastream mtdata -lp en-de Statmt-news_commentary-16-deu-eng,Statmt-europarl-10-deu-eng

    Example #1 mixes two datasets with equal weights (i.e., 1:1).
    Example #2 mixes two datasets with 1:2 ratio respectively.
    Example #3 simply concatenates both datasets separated by comma into a single dataset.
       Therefore, the resulting mixture weights are proportional to the number of segments in each dataset.

    The `--langs|-lp <src>-<tgt>` argument is used to enforce compatibility between the specified datasets and ensure correct ordering of source and target languages
    """

    def __init__(
        self,
        data_ids: List[str],
        mix_weights: Optional[List[float]] = None,
        langs: Tuple[str, str] = None,
        **kwargs,
    ):
        """Initialize mtdata pipeline.

        :param data_ids: List of mtdata IDs
        :param mix_weights: Mixture weights, defaults to None (i.e., equal weights)
        :param langs: Tuple of source and target language codes to enforce compatibility with specified dataset ids,
            defaults to None (not enforced)
        """
        if not mix_weights:
            mix_weights = [1.0] * len(data_ids)
        kwargs.pop('data_sources', None)
        super().__init__(mix_weights=mix_weights, data_sources=data_ids, **kwargs)
        assert len(data_ids) == len(
            self.mix_weights
        ), f'Expected {len(mix_weights)} weights, got {len(data_ids)}. See --mix-weights argument'

        random.seed(self.seed)
        if self.num_workers > 1:
            logger.warning(f'num_workers > 1 is not supported for MTData pipeline.')

        data_sources = []
        for data_id in data_ids:
            dids = data_id.split(',')  # allow comma-separated list of dataset IDs
            data_sources.append(MTDataSource(dids, langs=langs))

        if len(data_sources) > 1:
            stream = Mixer(data_sources, self.mix_weights)
        else:
            stream = data_sources[0]
        self.stream = BitextFilter(stream)  # removes all but fields 0 and 1

    @classmethod
    def get_data_sources_for_argparse(cls):
        help_msg = '''MTData dataset IDs which are of format Group-name-version-lang1-lang2
    E.g. "Statmt-news_commentary-16-deu-eng"
Run "mtdata list -id -l <src>-<tgt>" to list all available dataset IDs for any <src>-<tgt> language pair.
'''
        return [('data_ids', help_msg, '+')]

    @classmethod
    def get_data_sources_default_weights(cls):
        # we dont know how many sources will be provided until runtime CLI parsing
        return ['+']

    @classmethod
    def add_cli_args(cls, parser):
        super().add_cli_args(parser)

        def LangPair(txt) -> Tuple[str, str]:
            """Parse language pair from CLI argument."""
            pair = txt.split('-')
            assert len(pair) == 2, f'Expected 2 languages src-tgt, got {len(pair)}'
            return tuple(pair)

        parser.add_argument(
            '--langs',
            '-lp',
            required=True,
            metavar='SRC-TGT',
            type=LangPair,
            help='''Source and language order, e.g. "deu-eng". Ensures the correct order of the fields in the output.
            As per mtdata, language code 'mul' is special and meant for multilingual datasets.
            E.g. "mul-en" is compatible for x->en datasets, where as "en-mul" is for en->x for any x.''',
        )


def MTDataSource(
    dids: Union[str, List[str]],
    langs=None,
    progress_bar=False,
) -> Iterator[Line]:
    """MTData dataset iterator.

    :param dids: either a single dataset ID or a list of dataset ID.
        IDs are of form  Group-name-version-lang1-lang2 e.g. "Statmt-news_commentary-16-deu-eng"
    :param langs: source-target language order, e.g. "deu-eng"
    :progress_bar: whether to show progress bar
    :return: Line objects
    """
    from mtdata.data import INDEX, Cache, Parser, DatasetId
    from mtdata import cache_dir as CACHE_DIR, pbar_man
    from mtdata.iso.bcp47 import bcp47, BCP47Tag

    pbar_man.enabled = bool(progress_bar)

    if langs:  # check compatibility
        assert len(langs) == 2, f'Expected 2 languages, got {langs}'
        langs = (bcp47(langs[0]), bcp47(langs[1]))

    data_spec = []
    for did in dids:
        did = DatasetId.parse(did)
        assert did in INDEX, f'Unknown dataset ID: {did}'

        is_swap = False
        if langs:
            is_compat, is_swap = BCP47Tag.check_compat_swap(langs, did.langs)
            if not is_compat:
                langs_txt = '-'.join(map(str, langs))
                raise ValueError(f'{did} is not compatible with {langs_txt}.')
        entry = INDEX[did]
        path = Cache(CACHE_DIR).get_entry(entry)
        parser = Parser(path, ext=entry.in_ext or None, ent=entry)
        data_spec.append([did, parser, is_swap])
    count = 0
    delim = '\t'
    while True:
        for did, parser, is_swap in data_spec:
            for rec in parser.read_segs():
                if isinstance(rec, (list, tuple)):
                    fields = [col.replace(delim, ' ').replace('\n', ' ').strip() for col in rec]
                else:
                    fields = rec.split(delim)
                assert len(fields) >= 2, f'Expected 2 fields, got {len(fields)}'
                fields = fields[:2]
                if is_swap:
                    fields = [fields[1], fields[0]]
                yield Line(fields=fields)
                count += 1
