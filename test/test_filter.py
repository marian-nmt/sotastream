# -*- coding: utf-8 -*-

import os
import sys

sys.dont_write_bytecode = True

import pytest

from sotastream.data import Line
from sotastream.filters import *

from test_augmentors import ToLines, TEST_CORPUS

URL_CORPUS = [
    "http://microsoft.com\thttp://microsoft.com",
    "No URL here\tNo URL here",
    "http://google.com in the US\thttp://microsoft.com in the US",
]


from test_line import inputs


@pytest.mark.parametrize("corpus", [inputs, TEST_CORPUS, TEST_CORPUS, URL_CORPUS])
def test_bitext_filter(corpus):
    """
    Test that the bitext filter is working. It reduces a line object to just the first
    two fields.
    """

    for line, wholeline, bitextline in zip(corpus, ToLines(corpus), BitextFilter(ToLines(corpus))):
        length = len(line.split("\t"))

        assert len(wholeline) == length
        assert len(bitextline) == min(length, 2)
