# -*- coding: utf-8 -*-
"""
At the moment, this just checks that the pipelines actually run,
and that they return two fields only; it doesn't test that they
are correct in terms of the distributions they return. But this at
least prevents breaking the pipeline builds.
"""


import sys

sys.dont_write_bytecode = True

import pytest
import tempfile
from typing import List

from sotastream import Defaults
from sotastream.data import Line
from sotastream.pipelines import Pipeline
from sotastream.augmentors import *

from test_augmentors import TEST_CORPUS, ToLines


PIPELINES = [
    ("default", [TEST_CORPUS]),  # parallel only
    ("example", [TEST_CORPUS, TEST_CORPUS]),
]


def create_pipeline(pipeline_name, data_sources: List[List[str]]):
    """
    Creates a pipeline by creating temporary files from each of the data_sources,
    since DataSource expects file paths.
    """
    tmpdir = tempfile.TemporaryDirectory()

    data_files = []
    for source in data_sources:
        tmpfile = tempfile.NamedTemporaryFile("wt", suffix=".gz", dir=tmpdir.name, delete=False)
        data_files.append(tmpdir.name)
        with gzip.open(tmpfile.name, "wt") as outfh:
            for line in source:
                print(line, file=outfh)

    args = {
        "buffer_size": 2,
        "separator": Defaults.SEPARATOR,
        "doc_separator": Defaults.DOC_SEPARATOR,
        "max_tokens": 250,
        "sample_length": True,
        "doc_prob": 1.0,
        "doc_prob_parallel": 0.0,
        "mix_weights": [1] * len(data_files),
        "augment": False,
        'data_sources': data_files,
    }

    pipeline = Pipeline.create(pipeline_name, *data_files, **args)

    return pipeline, tmpdir


def cleanup_pipeline(tmpdir):
    tmpdir.cleanup()


@pytest.mark.parametrize("name, data_sources", PIPELINES)
def test_pipeline(name, data_sources):
    pipeline, data_files = create_pipeline(name, data_sources)

    for lineno, line in enumerate(pipeline, 1):
        if lineno > 10:
            break

    cleanup_pipeline(data_files)
