# -*- coding: utf-8 -*-

import os
import sys

sys.dont_write_bytecode = True

import pytest

from sotastream.data import Line

from test_augmentors import TEST_CORPUS, ToLines
from itertools import zip_longest
from copy import copy


def test_line():
    text = "Das ist ein Test\tThis is a test."
    source, target = text.split("\t")

    line = Line(text)
    assert line[0] == source
    assert line[1] == target


inputs = [
    "",
    "\tJust the target side, please.",
    "Nur die Quellseite, bitte\t",
    "Just an ambiguous sentence that should be rendered as the source",
    "This has\tlots of\tfields\tthat do not get\tprinted",
    "Here are a\tnumber of\tfields that I hope will\tbe joined\t.",
]


@pytest.mark.parametrize("text", inputs)
def test_str(text):
    line = Line(text)
    assert str(line) == text
    assert len(line) == len(text.split("\t"))
