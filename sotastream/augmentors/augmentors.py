import os
import gzip
import string
import random
import logging
from typing import Iterator, Iterable, Callable
from subprocess import Popen, PIPE

import titlecase
from infinibatch.datasets import chunked_dataset_iterator

from sotastream.data import Line
from sotastream import Defaults


logger = logging.getLogger(f"sotastream")


def UTF8File(path: str) -> Iterator[str]:
    """
    Opens a file and returns a stream of Line objects.
    """
    with open(path, "rb") as f:
        data = f.read()
        if path.endswith('.gz'):
            data = gzip.decompress(data)

    for line in data.decode(encoding='utf-8').splitlines():
        yield Line(line)


def enumerate_files(dir: str, ext: str):
    return [
        os.path.join(dir, path.name)
        for path in os.scandir(dir)
        if path.is_file() and (ext is None or path.name.endswith(ext))
    ]


def DataSource(
    path: str,
    processChunk: Callable = UTF8File,
    ext: str = ".gz",
    buffer_size: int = Defaults.BUFFER_SIZE,
    seed: int = 1234,
    shuffle: bool = True,
    worker_id: int = 0,
    num_workers: int = 1,
):
    """
    Creates an infinibatch data source from a directory of files that all
    have extension {ext}.

    :param path: directory containing chunks
    :param processChunk: function to call on each chunk
    :param ext: the file extension to glob over
    :param buffer_size: how many lines infinibatch loads into memory at a time
    :param seed: the random seed
    :param shuffle: whether to shuffle results across shards
    :param worker_id: For multiprocessing, this worker's ID (0-based)
    :param num_workers: For multiprocessing, the number of workers
    """

    # This is used to ensure that infinibatch iterators (a) differ on each node
    # and (b) see the same data in the same order, when called multiple times.
    # However, having multiple workers on a single node breaks this, because they
    # write to their shared queue in an unpredictable order. To fix this, we'd have
    # to do round-robin on the queue.
    if "OMPI_COMM_WORLD_SIZE" in os.environ:
        num_instances = int(os.environ["OMPI_COMM_WORLD_SIZE"])
        instance_rank = int(os.environ["OMPI_COMM_WORLD_RANK"])
        logger.info(f"Opening path {path} on instance {instance_rank} out of {num_instances} instances")
    else:
        num_instances = 1
        instance_rank = 0
        logger.info(f"Opening path {path}")

    random.seed(seed)

    # Worker ID i will only see every ith chunk
    chunk_file_paths = []
    total_chunks = 0
    subpaths = enumerate_files(path, ext)
    for pathno, subpath in enumerate(subpaths):
        total_chunks += 1
        if len(subpaths) < num_workers or pathno % num_workers == worker_id:
            chunk_file_paths.append(subpath)
    chunk_file_paths.sort()  # make sure file order is always the same, independent of OS

    logger.info(f"Worker {worker_id} gets {len(chunk_file_paths)} / {total_chunks} segments in path {path}")
    ds = chunked_dataset_iterator(
        chunk_refs=chunk_file_paths,
        read_chunk_fn=processChunk,
        shuffle=shuffle,
        buffer_size=buffer_size,
        seed=seed,
        use_windowed=False,
        num_instances=num_instances,
        instance_rank=instance_rank,
    )

    return ds


class Mixer:
    def __init__(self, iterators, probs):
        self.iterators = iterators
        self.probs = probs

    def __iter__(self):
        return self

    def __next__(self):
        draw = random.uniform(0, 1)
        prob_sum = 0
        for i, prob in enumerate(self.probs):
            prob_sum += prob
            if draw <= prob_sum:
                return next(self.iterators[i])

        return next(self.iterators[0])  # default


def Identity(lines):
    for line in lines:
        yield line


def Append(lines, functor):
    for line in lines:
        line[len(line)] = functor(line)
        yield line


def canBeUppercased(inputString):
    """Check if the input string can be plausibly uppercased (is the uppercased version different from the non-uppercased one).
    We randomly sample 10 chars (with repetition if needed) which should be good enough. Note, this is rather meant as a quick
    way to identify if a script has casing rather than if a particular string in a script with casing can be uppercased. Both
    may be caught."""
    if not inputString:
        return False
    randChars = "".join(random.choices(inputString, k=10))
    return randChars.upper() != randChars


def canBeLowercased(inputString):
    """Check if the input string can be plausibly lowercased (is the lowercased version different from the non-lowercased one).
    We randomly sample 10 chars (with repetition if needed) which should be good enough. Note, this is rather meant as a quick
    way to identify if a script has casing rather than if a particular string in a script with casing can be lowercased. Both
    may be caught."""
    if not inputString:
        return False
    randChars = "".join(random.choices(inputString, k=10))
    return randChars.lower() != randChars


def ToUpper(lines, fields=[0, 1], check=None):
    """Uppercases all specified fields. If check is set to a field id it conditions the uppercasing
    of the entire set on the fact if the checked field can be plausibly uppercased. This is used for
    things like Chinese source that has no case and would result in random target casing during inference"""
    for line in lines:
        if check is None or canBeUppercased(line[check]):
            for field in fields:
                line[field] = line[field].upper()
        yield line


def ToLower(lines, fields=[0, 1], check=None):
    """Lowercases all specified fields. If check is set to a field id it conditions the lowercasing
    of the entire set on the fact if the checked field can be plausibly lowercased."""
    for line in lines:
        if check is None or canBeLowercased(line[check]):
            for field in fields:
                line[field] = line[field].lower()
        yield line


def ToTitle(lines, fields=[0, 1], check=None):
    """Titlecases all specified fields. If check is set to a field id it conditions the titlecasing
    of the entire set on the fact if the checked field can be plausibly uppercased."""
    for line in lines:
        if check is None or canBeUppercased(line[check]):
            for field in fields:
                line[field] = titlecase.titlecase(line[field])
        yield line


def Tagger(lines, tag="", fields=[0]):
    for line in lines:
        for field in fields:
            line[field] = tag + line[field]
        yield line


def Copy(lines, from_field=1, to_field=0):
    for line in lines:
        line[to_field] = line[from_field]
        yield line


def CopySource(lines):
    """Copy source field to target."""
    return Copy(lines, 0, 1)


def Multiply(lines, n=2):
    """Makes n copies of the underlying object."""
    for line in lines:
        for field in range(1, n):
            line[field] = line[0]
        yield line


def JustSourceTarget(lines):
    """Removes all but fields 0 and 1"""
    for line in lines:
        yield Line(str(line))


def SPMEncoder(lines, spm_model):
    """Runs the SPM encoder on fields 0 and 1"""
    for line in lines:
        line[0:2] = list(map(lambda x: " ".join(x), spm_model.encode(line[0:2], out_type=str)))
        yield line


def SPMDecoder(lines, spm_model):
    """SPM decodes fields 0 and 1"""
    for line in lines:
        line[0:2] = spm_model.decode(list(map(str.split, line[0:2])))
        yield line
