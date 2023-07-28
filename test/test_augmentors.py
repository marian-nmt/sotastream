# -*- coding: utf-8 -*-

import sys
import os
import pytest
import random
from typing import Iterable, List

from sotastream.data import Line
from sotastream.augmentors import *

from collections import Counter

TEST_CORPUS = [
    "München 1856: Vier Karten, die Ihren Blick auf die Stadt verändern	Munich 1856: Four maps that will change your view of the city",
    "Eine Irren-Anstalt, wo sich heute Jugendliche begegnen sollen.	A mental asylum, where today young people are said to meet.",
    "Eine Gruftkapelle, wo nun für den S-Bahn-Tunnel gegraben wird.	A crypt chapel, where they are now digging tunnels for the S-Bahn.",
    "Kleingärtner bewirtschaften den einstigen Grund von Bauern.	Allotment holders cultivate the soil of former farmers.",
    "Die älteste offizielle Karte Münchens fördert spannende Geschichten zu Tage.	The oldest official map of Munich brings captivating stories to light.",
    "Es nervt, wenn Landkarten nicht aktuell sind.	It is annoying when geographical maps are not up-to-date.",
    "Das kennt jeder, der sich schon mal aufregen musste, weil das Auto-Navi statt einer Umgehungsstraße eine grüne Wiese anzeigte.	Anyone who has ever got worked up because the car's sat-nav is showing a green field instead of a bypass knows that.",
    "Die historischen Landkarten des digitalen Bayern-Atlases, ein Angebot des Geoportals Bayern der Staatsregierung, sind alles andere als aktuell - doch gerade deshalb sehr aufschlussreich.	The historical maps of the digital BayernAtlas, an offering from the State Government's Geoportal Bayern, are anything but up-to-date – and yet it is precisely for this reason that they are so informative.",
    "Besonders wenn man sie mit aktuellen Online-Karten vergleicht.	Especially when one compares them with current online maps.",
    "Dann wird deutlich, wie sich Städte und Gemeinden im Verbreitungsgebiet des Münchner Merkur seit dem 19. Jahrhundert verändert haben.	Then it becomes clear how the towns and municipalities in the distribution area of Munich's Merkur newspaper have changed since the 19th century.",
]


# ToLines should be a generator since everything else is generators or iterators
def ToLines(lines: List[str]) -> Iterable[Line]:
    for x in lines:
        yield Line(x)


def test_line_creation():
    lines = ToLines(TEST_CORPUS)

    for lineno, line in enumerate(lines):
        assert len(line) == 2, f"wrong length for {lineno}: {line.fields[2]}"


# we need to call ToLines here twice to create a new generator, otherwise zipping would advance the same generator twice
def test_identity():
    lines1 = ToLines(TEST_CORPUS)
    lines2 = ToLines(TEST_CORPUS)

    for line, modline in zip(lines1, Identity(lines2)):
        assert line == modline


def test_tolower():
    for line in ToLower(ToLines(TEST_CORPUS)):
        assert line[0].islower() and line[1].islower()

    # Make sure lowering source works
    for line in ToLower(ToLines(TEST_CORPUS), fields=[0]):
        assert line[0].islower() and not line[1].islower()

    # Lower target
    for line in ToLower(ToLines(TEST_CORPUS), fields=[1]):
        assert not line[0].islower() and line[1].islower()


def test_toupper():
    for line in ToUpper(ToLines(TEST_CORPUS)):
        assert line[0].isupper() and line[1].isupper()

    # Make sure uppering source works
    for line in ToUpper(ToLines(TEST_CORPUS), fields=[0]):
        assert line[0].isupper() and not line[1].isupper()

    # Upper target
    for line in ToUpper(ToLines(TEST_CORPUS), fields=[1]):
        assert not line[0].isupper() and line[1].isupper()

    # Only uppercase if field `check` can be uppercased
    for line in ToUpper(ToLines(["她是囚犯还是老板?	Is she prisoner or boss?"]), fields=[0, 1], check=1):
        assert not line[0].isupper() and line[1].isupper()

    for line in ToUpper(ToLines(["她是囚犯还是老板?	Is she prisoner or boss?"]), fields=[0, 1], check=0):
        assert not line[0].isupper() and not line[1].isupper()

    for line in ToUpper(ToLines(["她是囚犯还是老板?	Is she prisoner or boss?"]), fields=[0, 1]):
        assert not line[0].isupper() and line[1].isupper()


@pytest.mark.parametrize("n", range(2, 5))
def test_multiply(n):
    for line in Multiply(ToLines(TEST_CORPUS), n=n):
        for i in range(1, n):
            assert line[0] == line[i]


######################################################################################

DIACRITICIZED_CORPUS = [
    "ąćęłńóśźż ĄĆĘŁŃÓŚŹŻ\tąćęłńóśźż",
    "áčďéě íňóřš ťúůýž ÁČĎÉĚ ÍŇÓŘŠ ŤÚŮÝŽ\táčďéě íňóřš ťúůýž",
    "äöü ÄÖÜ\täöü",
    "ç é âêîôû àèìòù ëïü Ç É ÂÊÎÔÛ ÀÈÌÒÙ ËÏÜ\tç é âêîôû àèìòù ëïü",
    "áéíóú ñ ü ÁÉÍÓÚ Ñ Ü\táéíóú ñ ü",
]

TRAILING_PUNCT_CORPUS = [
    "This is a test sentence.\t这是一个测试语句。",
    "This is a test sentence!\t这是一个测试语句！",
    "This is a test sentence.\tこれはテスト文です。",
    "This is a test sentence;\tこれはテスト文です！",
    "This is a test sentence.\t테스트 문장입니다.",
    "This is a test sentence;\tهذه جملة اختبار.",
    "这是一个测试语句。\tThis is a test sentence." "这是一个测试语句！\tThis is a test sentence!",
    "これはテスト文です。\tThis is a test sentence.",
    "これはテスト文です！\tThis is a test sentence;",
    "테스트 문장입니다.\tThis is a test sentence.",
    "هذه جملة اختبار.\tThis is a test sentence;",
]


######################################################################################

# char-based alignments for input above
TEST_CORPUS_ALN = [
    [
        ((0, 9), (0, 7)),
        ((9, 10), (7, 8)),
        ((10, 11), (8, 9)),
        ((11, 12), (9, 10)),
        ((12, 13), (10, 11)),
        ((13, 15), (11, 13)),
        ((15, 20), (13, 18)),
        ((20, 26), (18, 23)),
        ((26, 28), (18, 23)),
        ((28, 32), (23, 28)),
        ((32, 38), (28, 33)),
        ((38, 44), (33, 40)),
        ((44, 48), (40, 45)),
        ((48, 52), (45, 50)),
        ((52, 58), (50, 53)),
        ((58, 68), (53, 57)),
    ],
    [
        ((0, 5), (0, 2)),
        ((5, 8), (9, 11)),
        ((8, 10), (11, 13)),
        ((10, 11), (11, 13)),
        ((11, 18), (15, 17)),
        ((18, 20), (15, 17)),
        ((18, 20), (17, 23)),
        ((20, 23), (23, 29)),
        ((23, 28), (29, 35)),
        ((28, 34), (35, 42)),
        ((34, 45), (42, 46)),
        ((46, 54), (46, 51)),
        ((54, 55), (51, 54)),
        ((55, 61), (54, 58)),
        ((61, 62), (58, 59)),
    ],
    [
        ((0, 5), (0, 2)),
        ((5, 6), (2, 7)),
        ((6, 10), (7, 8)),
        ((10, 17), (8, 14)),
        ((17, 19), (14, 16)),
        ((19, 22), (14, 16)),
        ((22, 26), (16, 22)),
        ((26, 31), (22, 27)),
        ((31, 35), (27, 31)),
        ((35, 36), (35, 38)),
        ((35, 36), (59, 60)),
        ((36, 37), (38, 43)),
        ((37, 41), (61, 65)),
        ((41, 42), (49, 51)),
        ((41, 42), (60, 61)),
        ((42, 48), (43, 49)),
        ((42, 48), (55, 59)),
        ((48, 49), (7, 8)),
        ((58, 62), (61, 65)),
        ((62, 63), (65, 66)),
    ],
    [
        ((0, 5), (0, 2)),
        ((5, 6), (2, 5)),
        ((6, 9), (2, 5)),
        ((9, 10), (5, 10)),
        ((10, 14), (10, 18)),
        ((14, 16), (10, 18)),
        ((14, 16), (27, 28)),
        ((16, 26), (18, 27)),
        ((26, 29), (27, 28)),
        ((29, 33), (28, 32)),
        ((33, 36), (28, 32)),
        ((36, 38), (32, 37)),
        ((38, 43), (37, 40)),
        ((43, 49), (40, 47)),
        ((49, 53), (40, 47)),
        ((53, 59), (47, 54)),
        ((59, 60), (54, 55)),
    ],
    [
        ((0, 4), (0, 4)),
        ((4, 13), (4, 11)),
        ((13, 24), (11, 20)),
        ((24, 30), (20, 24)),
        ((30, 38), (24, 27)),
        ((38, 40), (27, 34)),
        ((38, 40), (34, 41)),
        ((40, 49), (41, 44)),
        ((49, 57), (44, 47)),
        ((57, 59), (47, 53)),
        ((59, 71), (53, 61)),
        ((71, 74), (61, 64)),
        ((74, 78), (64, 69)),
        ((78, 79), (69, 70)),
    ],
    [
        ((0, 3), (0, 3)),
        ((3, 7), (3, 6)),
        ((7, 8), (6, 15)),
        ((7, 8), (15, 20)),
        ((8, 10), (20, 33)),
        ((10, 15), (33, 38)),
        ((15, 19), (38, 42)),
        ((19, 26), (42, 46)),
        ((26, 32), (46, 48)),
        ((32, 39), (48, 49)),
        ((32, 39), (49, 51)),
        ((39, 40), (51, 52)),
        ((40, 44), (52, 56)),
        ((44, 45), (56, 57)),
    ],
    [
        ((0, 4), (0, 7)),
        ((4, 9), (7, 11)),
        ((10, 15), (11, 15)),
        ((15, 17), (15, 20)),
        ((17, 21), (20, 24)),
        ((21, 26), (24, 31)),
        ((26, 32), (31, 34)),
        ((66, 67), (55, 56)),
        ((71, 72), (59, 60)),
        ((101, 106), (93, 96)),
        ((106, 113), (96, 98)),
        ((113, 117), (100, 105)),
        ((117, 119), (100, 105)),
        ((119, 121), (105, 111)),
        ((121, 127), (111, 115)),
        ((127, 128), (115, 116)),
    ],
    [
        ((42, 48), (35, 41)),
        ((48, 49), (125, 126)),
        ((49, 54), (41, 46)),
        ((56, 58), (46, 48)),
        ((74, 77), (88, 91)),
        ((77, 83), (91, 98)),
        ((83, 85), (86, 88)),
        ((185, 186), (206, 207)),
    ],
    [
        ((0, 10), (0, 11)),
        ((10, 15), (11, 16)),
        ((15, 19), (16, 20)),
        ((19, 23), (16, 20)),
        ((23, 27), (20, 27)),
        ((27, 37), (27, 29)),
        ((37, 43), (29, 34)),
        ((43, 44), (34, 39)),
        ((44, 51), (39, 47)),
        ((51, 54), (39, 47)),
        ((54, 60), (47, 54)),
        ((60, 61), (54, 58)),
        ((61, 62), (58, 59)),
    ],
    [
        ((0, 5), (0, 5)),
        ((5, 10), (5, 8)),
        ((10, 18), (8, 16)),
        ((18, 20), (16, 22)),
        ((20, 24), (22, 26)),
        ((24, 29), (26, 30)),
        ((87, 90), (92, 95)),
        ((90, 93), (95, 98)),
        ((93, 94), (98, 99)),
        ((103, 104), (132, 133)),
        ((104, 105), (128, 132)),
        ((104, 105), (133, 134)),
        ((107, 119), (133, 134)),
        ((119, 130), (134, 137)),
        ((130, 135), (137, 144)),
        ((135, 136), (144, 145)),
    ],
]

# output for corpus above after phrasefixing, randomly seeded by string hash of input, hence should be deterministic
TEST_CORPUS_PHRASEFIX_OUT = [
    "München 1856(phrasefix)(#2)(#2)(#phrasefix) Vier (phrasefix)(#8)(#9)(#phrasefix) Blick auf (phrasefix)(#2)(#8)(#phrasefix)	Munich 1856(phrasefix)(#2)(#2)(#phrasefix) Four (phrasefix)(#8)(#9)(#phrasefix) change your (phrasefix)(#2)(#8)(#phrasefix) city",
    "(phrasefix)(#1)(#3)(#phrasefix) (phrasefix)(#2)(#6)(#phrasefix) wo sich heute (phrasefix)(#7)(#9)(#phrasefix) begegnen sollen.	(phrasefix)(#1)(#3)(#phrasefix) (phrasefix)(#2)(#6)(#phrasefix) today young people (phrasefix)(#7)(#9)(#phrasefix) said to meet.",
    "(phrasefix)(#2)(#8)(#phrasefix) Gruftkapelle, wo (phrasefix)(#2)(#0)(#phrasefix) (phrasefix)(#8)(#2)(#phrasefix) S-Bahn-Tunnel gegraben wird.	(phrasefix)(#2)(#8)(#phrasefix) crypt chapel, (phrasefix)(#2)(#0)(#phrasefix) (phrasefix)(#8)(#2)(#phrasefix) digging tunnels for the S-Bahn.",
    "Kleingärtner bewirtschaften (phrasefix)(#8)(#7)(#phrasefix)(phrasefix)(#5)(#2)(#phrasefix)	Allotment holders cultivate (phrasefix)(#8)(#7)(#phrasefix)(phrasefix)(#5)(#2)(#phrasefix)",
    "Die älteste offizielle Karte Münchens fördert spannende Geschichten zu (phrasefix)(#0)(#3)(#phrasefix).	The oldest official map of Munich brings captivating stories to (phrasefix)(#0)(#3)(#phrasefix).",
    "Es nervt, (phrasefix)(#2)(#0)(#phrasefix) nicht aktuell (phrasefix)(#5)(#8)(#phrasefix)	It is annoying when geographical (phrasefix)(#2)(#0)(#phrasefix) up-to(phrasefix)(#5)(#8)(#phrasefix)",
    "Das kennt jeder, der sich schon mal aufregen musste, weil das (phrasefix)(#4)(#6)(#phrasefix) einer Umgehungsstraße eine grüne Wiese anzeigte.	Anyone who has ever got worked up because the (phrasefix)(#4)(#6)(#phrasefix)nav is showing a green field instead of a bypass knows that.",
    "Die historischen Landkarten des digitalen Bayern(phrasefix)(#9)(#7)(#phrasefix)Atlases(phrasefix)(#9)(#1)(#phrasefix) ein Angebot (phrasefix)(#6)(#5)(#phrasefix), sind alles andere als aktuell - doch gerade deshalb sehr aufschlussreich.	The historical maps of the digital BayernAtlas(phrasefix)(#9)(#1)(#phrasefix) an offering from the State (phrasefix)(#6)(#5)(#phrasefix) Bayern, are anything but (phrasefix)(#9)(#7)(#phrasefix) – and yet it is precisely for this reason that they are so informative.",
    "(phrasefix)(#8)(#3)(#phrasefix) (phrasefix)(#2)(#3)(#phrasefix) (phrasefix)(#7)(#2)(#phrasefix)(phrasefix)(#1)(#6)(#phrasefix)Karten vergleicht(phrasefix)(#2)(#0)(#phrasefix)	(phrasefix)(#8)(#3)(#phrasefix) (phrasefix)(#2)(#3)(#phrasefix) (phrasefix)(#7)(#2)(#phrasefix) (phrasefix)(#1)(#6)(#phrasefix) current online maps(phrasefix)(#2)(#0)(#phrasefix)",
    "Dann wird deutlich, wie sich Städte und Gemeinden im Verbreitungsgebiet des Münchner (phrasefix)(#2)(#9)(#phrasefix) 19. Jahrhundert verändert haben.	Then it becomes clear how the towns and municipalities in the distribution area of Munich's (phrasefix)(#2)(#9)(#phrasefix) changed since the 19th century.",
]


# fake aligner object to be used in PhraseFixer test
class FakeAligner:
    def __init__(self, alignment):
        self.alignment = alignment
        self.lineNo = 0

    # This produces a sequence of character span pairs of aligned char ranges from raw src to trg string
    def align_char_ranges(self, src, trg):
        ret = self.alignment[self.lineNo]
        self.lineNo += 1
        return ret


def test_mixer(num_trials=100000):
    """Ensures that the mixer chooses from streams evenly."""

    def gen(token):
        while True:
            yield token

    mixer = Mixer([gen("a"), gen("b"), gen("c")], [1 / 3, 1 / 3, 1 / 3])

    counter = Counter()
    for i, value in enumerate(mixer):
        counter[value] += 1
        if i > num_trials:
            break

    values = counter.values()
    assert max(values) - min(values) <= 0.01 * num_trials
