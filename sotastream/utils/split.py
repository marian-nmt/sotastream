#!/usr/bin/env python3

import datetime
import gzip
import hashlib
import logging
import os
import shutil
import subprocess
import time

from pathlib import Path
from typing import Type

from sotastream.pipelines import PIPELINES


logger = logging.getLogger(f"sotastream")


# The block size to use when compute MD5 hashes
MD5_BLOCK_SIZE = 8192


def split_file_into_chunks(
    filepath: str,
    tmpdir: str = "/tmp/sotastream",
    split_size: int = 10000,
    native: bool = False,
    overwrite: bool = False,
) -> Path:
    """
    Splits a file into compressed chunks under a directory.
    The location will be in a directory named by the file's checksum, within the
    provided temporary directory. Results are cached, providing for quick restarting.

    :param filepath: The input file path
    :param tmpdir: The top-level temporary directory to write to
    :param split_size: The size of each chunk in lines
    :param native: If True, use Python to split, instead of a subshell
    :return: The directory where the chunks are stored, as a Path object
    """
    start_time = time.perf_counter()

    split_func = split_native if native else split_subshell

    # Compute the checksum
    md5sum = compute_md5(filepath)
    logger.info(f"md5sum({filepath}) = {md5sum} computed in {time.perf_counter() - start_time:.1f}s")

    # Check if we already have the file split
    destdir = Path(tmpdir) / md5sum
    donefile = destdir / ".done"
    if destdir.exists() and overwrite:
        logger.info(f"Removing existing split directory {destdir}")
        shutil.rmtree(destdir)
    elif donefile.exists():
        logger.info(f"Using cached splitting of {filepath} (checksum: {md5sum})")
        return destdir

    # If not, split the file
    logger.info(f"Splitting file {filepath} to {tmpdir}...")
    destdir.mkdir(parents=True, exist_ok=True)
    start_time = time.perf_counter()
    split_func(filepath, destdir, split_size)
    logger.info(f"File {filepath} splitting took {time.perf_counter() - start_time:.1f}s")

    with open(donefile, "w") as outfh:
        print(f"{filepath} finished splitting {datetime.datetime.now()}", file=outfh)

    return destdir


def split_native(filepath: str, destdir: Path, split_size: int):
    """
    Split directly in Python by reading the file.
    This version is slower than the subshell version.

    :param filepath: The input file path
    :param destdir: The output directory
    :param split_size: The size of each chunk in lines
    """

    def get_chunkpath(index=0):
        outfh = smart_open(destdir / f"part.{index:05d}.gz", "wt")
        index += 1
        return index, outfh

    with smart_open(filepath) as infh:
        chunkno, outfh = get_chunkpath()
        logger.info(f"Splitting {filepath} to {destdir}")
        for lineno, line in enumerate(infh, 1):
            line = line.rstrip("\r\n")
            if lineno % split_size == 0:
                if outfh is not None:
                    outfh.close()
                chunkno, outfh = get_chunkpath(chunkno)
            print(line, file=outfh)
        outfh.close()


def split_subshell(filepath: str, destdir: Path, split_size: int):
    """
    Split using a subshell (~8x faster).

    :param filepath: The input file path
    :param destdir: The output directory
    :param split_size: The size of each chunk in lines
    """
    cmd = f"pigz -cd {filepath} | sed 's/\r//g' | split -d -a5 -l {split_size} --filter 'pigz > $FILE.gz' - {destdir}/part."
    logger.info(cmd)
    subprocess.run(cmd, shell=True, check=True)


def smart_open(filepath: str, mode: str = "rt", encoding: str = "utf-8"):
    """Convenience function for reading and writing compressed or plain text files.

    :param filepath: The file to read.
    :param mode: The file mode (read, write).
    :param encoding: The file encoding.
    :return: a file handle.
    """
    if Path(filepath).suffix == ".gz":
        return gzip.open(filepath, mode=mode, encoding=encoding, newline="\n")
    return open(filepath, mode=mode, encoding=encoding, newline="\n")


def compute_md5(filepath: str):
    """Computes an MD5 checksum over a file.
    Note that binary reading in this way is as fast as a subshell call.

    :param filepath: The file path as as string
    :return: The checksum as a hexdigest.
    """
    with open(filepath, "rb") as f:
        m = hashlib.md5()
        while chunk := f.read(MD5_BLOCK_SIZE):
            m.update(chunk)
        return m.hexdigest()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "infile",
        type=str,
        help="Path to TSV input file containing source, target, and (optionally) docid fields",
    )
    parser.add_argument("--numlines", "-l", type=int, default=10000)
    parser.add_argument("--prefix-dir", "-p", default="/tmp/sotastream")
    args = parser.parse_args()

    logger.basicConfig(level=logging.INFO)

    split_file_into_chunks(args.infile, tmpdir=args.prefix_dir, split_size=args.numlines)
