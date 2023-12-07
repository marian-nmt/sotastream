# Sotastream
[![image](http://img.shields.io/pypi/v/sotastream.svg)](https://pypi.python.org/pypi/sotastream/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)
[![Read the Docs](https://img.shields.io/readthedocs/sotastream.svg)](https://sotastream.readthedocs.io/)


Sotastream is a tool for data augmentation for training
pipeline. It uses `infinibatch` internally to generate an infinite
stream of shuffled training data and provides a means for on-the-fly
data manipulation, augmentation, mixing, and sampling.


## Setup

To install from PyPI (https://pypi.org/project/sotastream/)
```bash
pip install sotastream
```

*Developer Setup:*

```bash
# To begin, clone the repository:
git clone https://github.com/marian-nmt/sotastream
cd sotastream
# option 1:
python -m pip install .
# option 2: install in --editable mode
python -m pip install -e .
```

*Entry points*
* As a module:  `python -m sotastream`
* As a bin in your $PATH: `sotastream`

## Development

Install development tools
```bash
python -m pip install -e .[dev,test]   # editable mode
```
Editable mode (`-e / --editable`) is recommended for development purposes, `pip` creates symbolic link to your source code in a way that any edits made are reflected directly to the installed package. `[dev,test]` installs depencies for development and tests which includes `black`, `pytest` etc.

We use `black` to reformat code to a common code style.
```bash
make reformat
```

Before creating any pull requests, run
```bash
make check          # runs reformatter and tests
```

## Running tests

```bash
make test           # run unit tests
make regression     # run regression tests
```

 See `Makefile` for more details.


## Usage examples

A folder like `split/parallel` contains training data in tsv format (`src<tab>tgt`) split into 
`*.gz` files of around 100,000 lines for better shuffling. The below will output an infinite
stream of data generated from the gzipped files in these folders, according to the "wmt" recipe 
found in `sotastream/pipelines/example_pipeline.py`.

```
python -m sotastream example split/parallel split/backtrans
```
You can also provide compressed TSV files directly, in which case sotastream will split them
to checksummed folders under `/tmp/sotastream/{checksum}`:

```
python -m sotastream example parallel.tsv.gz backtrans.tsv.gz
```

There are currently two main pipelines: "default", and "wmt". These vary according to
the data sources they take as well as the other options available to them.

There are global options that control behavioral aspects such as splitting and parallelization,
and also pipeline-specific arguments. You can see these by running

```
# see global options
python -m sotastream -h

# see default pipeline options
python -m sotastream default -h

# see wmt pipeline options
python -m sotastream wmt -h
```

## Don't cross the streams!

Sotastream workflows build a directed acyclic graph (DAG)
consisting of cascades of generators that pass through mutable lines
from the graph inputs to the pipeline output. Since each step provides
transformations and manipulations of each input line, the only
requirement is that modifications along separate branches must not be
merged into a single node in the graph, or at least, that great care 
should be taken when doing so. An example is the Mixer, which 
does not actually merge modifications from alternate branches, but instead
selects across multiple incoming branches using a provided probability
distribution.

# Custom/private pipelines from own (private) directory

You can create a custom pipeline by adding a file in the current (invocation)
directory with a file name matching the pattern "*_pipeline.py". This should
follow the interface defined in `sotastream/pipelines`, namely:

* Call `@pipeline("name")` to give your pipeline a name. This name must not conflict with existing names.
* Inherit from `Pipeline` base class from `sotastream.pipeline`. For document pipelines, use `DocumentPipeline` as base class.

You can find some examples in `test/dummy_pipeline.py`, as well as the real examples in `sotastream/pipelines`.

# Authors

Sotastream is developed by _TextMT Team_ @ Microsoft Translator.

If you use this tool, please cite: 
Paper link: https://arxiv.org/abs/2308.07489  | https://aclanthology.org/2023.nlposs-1.13/


```bibtex
@inproceedings{post-etal-2023-sotastream,
    title = "{SOTASTREAM}: A Streaming Approach to Machine Translation Training",
    author = "Post, Matt  and
      Gowda, Thamme  and
      Grundkiewicz, Roman  and
      Khayrallah, Huda  and
      Jain, Rohit  and
      Junczys-Dowmunt, Marcin",
    editor = "Tan, Liling  and
      Milajevs, Dmitrijs  and
      Chauhan, Geeticka  and
      Gwinnup, Jeremy  and
      Rippeth, Elijah",
    booktitle = "Proceedings of the 3rd Workshop for Natural Language Processing Open Source Software (NLP-OSS 2023)",
    month = dec,
    year = "2023",
    address = "Singapore, Singapore",
    publisher = "Empirical Methods in Natural Language Processing",
    url = "https://aclanthology.org/2023.nlposs-1.13",
    pages = "110--119",
}
```
