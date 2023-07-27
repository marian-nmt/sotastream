from pathlib import Path
import pytest
from collections import defaultdict
import subprocess
import sys
import logging as log
from typing import List

log.basicConfig(level=log.INFO)
data_dir = Path(__file__).parent / "data/trec"

EXPECTED = """LOC	country	What European country is home to the beer-producing city of Budweis ?
HUM	ind	Who was the last woman executed in England ?
ABBR	exp	What does the word LASER mean ?
HUM	ind	What player squats an average of 3 times during a baseball doubleheader ?
LOC	other	Where can I find a world atlas map online at no charge ?
NUM	date	When did French revolutionaries storm the Bastille ?
LOC	other	On what avenue is the original Saks department store located ?
ENTY	color	What color is the cross on Switzerland 's flag ?
ABBR	exp	What does SHIELD stand for ?
LOC	country	What country has the best defensive position in the board game Diplomacy ?
ENTY	other	What five cards make up a perfect Cribbage hand ?
NUM	count	How many people die from snakebite poisoning in the U.S. per year ?
ABBR	exp	What does pH stand for ?
ENTY	body	Which leg does a cat move with its left front leg when walking - its left rear or right rear leg ?
DESC	def	What is a disaccharide ?
ENTY	symbol	What sign is The Water Carrier the zodiacal symbol for ?
HUM	ind	What well-known actor is the father of star Alan Alda ?
NUM	count	How many airline schools are there in the U.S. ?
LOC	city	What is the capital of Burkina Faso ?
HUM	ind	Who were the four famous founders of United Artists ?"""


def get_data_paths() -> List[str]:
    flag = data_dir / '._OK'
    if not flag.exists():
        url = "https://cogcomp.seas.upenn.edu/Data/QA/QC/train_5500.label"
        try:
            import requests
        except ImportError:
            pytest.skip("requests is unavailable. `pip install requests` to enable this test.")

        lines = requests.get(url).text.splitlines(keepends=False)
        data_parsed = defaultdict(list)
        for line in lines:
            label, text = line.split(' ', maxsplit=1)
            label, sublabel = label.split(':')
            data_parsed[label].append(f'{label}\t{sublabel}\t{text}')
        for label, texts in data_parsed.items():
            label_dir = data_dir / label
            label_dir.mkdir(parents=True, exist_ok=True)
            with open(label_dir / 'part00.tsv', 'wt') as outfh:
                for text in texts:
                    print(text, file=outfh)
        flag.touch()

    sub_dirs = set()
    for file in data_dir.glob('*/*.tsv'):
        sub_dirs.add(str(file.parent))

    return list(sorted(sub_dirs))


def test_multistream():
    """
    Test varargs pipeline.
    Starts a subprocess and reads its output.
    """
    paths = get_data_paths()
    assert len(paths) > 1
    base_cmd = f'{sys.executable} -m sotastream -n 1 -q 1000 -b 1000 --seed 43'
    cmd = f'{base_cmd} multistream {" ".join(paths)}'
    log.info(f'Running command: {cmd}')
    proc = subprocess.Popen(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
        stdin=subprocess.DEVNULL,
        text=True,
        bufsize=1,
    )

    try:
        expected = EXPECTED.split('\n')
        recieved = []
        # collect 50_000 lines and compute stats
        stats = defaultdict(int)
        max_stats = 50_000
        i = 0
        for line in proc.stdout:
            i += 1
            label = line.split('\t')[0]
            if i <= len(expected):
                recieved.append(line.rstrip('\n'))
                if i == len(expected):
                    if recieved != expected:
                        expected = "\n".join(expected)
                        recieved = "\n".join(recieved)
                        log.error(f'##Expected:\n{expected}\n\n##Got:\n{recieved}')
                        pytest.fail('Expected and recieved lines do not match.')
            stats[label] += 1
            if i > max_stats:
                break
        # all paths are used
        assert len(stats) == len(paths), f'Expected {len(paths)} paths to be used, got {len(stats)}.'
        majority, minority = max(stats.values()), min(stats.values())
        assert majority > 1
        # all paths are used ~equally; allow 5% difference between majority and minority
        assert (
            abs(majority - minority) < 0.05 * majority
        ), f'Paths are not used equally. majority: {majority}, minority: {minority}. Diff: {100 * (majority-minority)/majority:.2f}% difference.'
    finally:
        proc.terminate()
