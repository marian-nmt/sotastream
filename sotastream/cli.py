#!/usr/bin/env python3

import sys

# This might have to do with functioning on mounted Azure blobs
sys.dont_write_bytecode = True

import argparse
import logging
import json
import os
import time

from collections import defaultdict
from multiprocessing import Pipe, Process
from typing import Type

from . import __version__, Defaults
from .utils.split import split_file_into_chunks
from .pipelines import Pipeline, PIPELINES

# Use seed in logger for when multiple are running
logger = logging.getLogger(f"sotastream")

USER = os.environ.get('USER', os.environ.get('USERNAME', 'nouser'))


def adjustSeed(seed, local_num_instances, local_instance_rank):
    """
    Adjust seed for infinibatch such that each instance gets a different one based on process number and MPI
    coordinates.
    """
    if seed == 0:
        seed = round(time.time() * 1000)  # the current time in milliseconds

    mpi_num_instances = 1
    mpi_instance_rank = 0

    # these variables are set automatically by mpirun when used inside an MPI world
    if "OMPI_COMM_WORLD_SIZE" in os.environ:
        mpi_num_instances = int(os.environ["OMPI_COMM_WORLD_SIZE"])
        mpi_instance_rank = int(os.environ["OMPI_COMM_WORLD_RANK"])

    # hash-combine seed with local process number and rank and MPI process number and rank
    hashed_seed = hash((seed, local_num_instances, local_instance_rank, mpi_num_instances, mpi_instance_rank))

    logger.info(
        f"Computed seed {hashed_seed} from original seed {seed} and instance info: ({local_num_instances}, {local_instance_rank}, {mpi_num_instances}, {mpi_instance_rank})"
    )
    return hashed_seed


def run_pipeline_process(conn, args, seed, worker_id, num_workers):
    """
    Runs a pipeline in a single subprocess. Each subprocess writes to
    the pipe (conn) after it has seen the specified number (args.queue_buffer_size)
    of lines.
    """

    kwargs = {k: v for k, v in vars(args).items() if not (k in ["pipeline", "seed"])}

    # These environment variables are used in the subprocesses to determine which worker they are
    os.environ["SOTASTREAM_WORKER_ID"] = str(worker_id)
    os.environ["SOTASTREAM_WORKER_COUNT"] = str(num_workers)
    pipeline = Pipeline.create(args.pipeline, seed=seed, **kwargs)

    try:
        lines = []
        for line in pipeline:
            lines.append(str(line))
            if len(lines) >= min(args.queue_buffer_size, args.buffer_size):
                conn.send(lines)
                lines = []
        if lines:
            conn.send(lines)
    finally:
        conn.close()


def add_global_args(parser: argparse.ArgumentParser):
    """
    Add global arguments to the parser. These appear before the pipeline argument and are available
    to all pipelines.

    :param parser: The parser to add the options to.
    """
    parser.add_argument(
        "--log-rate", "-lr", type=int, default=0, metavar="N", help="Log every Nth instance (0=off)"
    )
    parser.add_argument(
        "--log-first",
        "-lf",
        type=int,
        default=5,
        metavar="N",
        help="Log first N instances (default: %(default)s)",
    )
    parser.add_argument("--sample-file", type=argparse.FileType("tw"), help="Where to log samples")
    parser.add_argument(
        '--buffer-size',
        '-b',
        help='Number of lines infinibatch will load into memory',
        type=int,
        default=Defaults.BUFFER_SIZE,
    )
    parser.add_argument(
        '--queue-buffer-size',
        '-q',
        help='Queue buffer size',
        type=int,
        default=Defaults.QUEUE_BUFFER_SIZE,
    )
    parser.add_argument(
        '--seed',
        '-s',
        help='Random seed (default 0 uses time for initialization)',
        type=int,
        default=Defaults.SEED,
    )
    parser.add_argument(
        '--num-processes',
        '-n',
        help='Number of processes to use for better throughput',
        type=int,
        default=Defaults.NUM_PROCESSES,
    )
    parser.add_argument('--version', '-V', action='version', version='sotastream {}'.format(__version__))
    parser.add_argument(
        "--split-tmpdir",
        default=f"/tmp/sotastream-{USER}",
        help="Base temporary directory to use when splitting data files",
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress logging output")


def maybe_split_files(args):
    """Split data files into smaller files in a temporary directory

    This function updates args inplace: it replaces .gz paths (if any) with split dirs.

    Args:
        args: CLI args object from argparse
    """
    # Look up the class for the pipeline, and get the named list of arguments
    PipelineClass: Type['Pipeline'] = PIPELINES[args.pipeline]
    args_dict = vars(args)
    data_source_params = PipelineClass.get_data_sources_for_argparse()
    # Use the name to get the path from the runtime args object
    data_sources = [(x[0], args_dict[x[0]]) for x in data_source_params]
    for name, path in data_sources:
        # For any path that is a .gz file, split it into chunks.
        # Directories that were pre-split are left as-is.
        if not isinstance(path, str):
            logger.warning(f"Skipping {name}={path} because it is {type(path)}, but str expected")
            continue
        if not os.path.isdir(path) and path.endswith(".gz"):
            splitdir = split_file_into_chunks(path, tmpdir=args.split_tmpdir, split_size=args.buffer_size)
            setattr(args, name, splitdir)
    # Inject a keyword argument 'data_sources' that contains all data sources
    setattr(args, 'data_sources', [path for name, path in data_sources])


def main():
    stats = defaultdict(int)
    stats['start_time'] = time.time()
    # Get the list of available pipelines
    parser = argparse.ArgumentParser(
        prog='sotastream',
        description='Command line wrapper for augmentation pipelines',
        formatter_class=argparse.RawTextHelpFormatter,
        epilog='''\n\nTo load additional pipelines create (or symlink) *_pipeline.py files from current directory.''',
    )
    add_global_args(parser)

    # Each pipeline is a different subcommand with its own arguments.
    sub_parsers = parser.add_subparsers(
        dest='pipeline',
        required=True,
        metavar="pipeline",
        help="The pipeline to run. Available pipelines:\n- " + "\n- ".join(sorted(PIPELINES.keys())),
    )
    for pipeline_name, pipeline_class in PIPELINES.items():
        # Create a sub-parser and add the pipeline's arguments to it.
        sub_parser = sub_parsers.add_parser(
            pipeline_name, description=pipeline_class.__doc__, formatter_class=argparse.RawTextHelpFormatter
        )
        pipeline_class.add_cli_args(sub_parser)

    args = parser.parse_args()
    logLevel = logging.CRITICAL if args.quiet else logging.INFO
    logging.basicConfig(level=logLevel)

    maybe_split_files(args)

    N = args.num_processes

    pipes = [Pipe() for i in range(N)]
    processes = [
        Process(target=run_pipeline_process, args=(pipes[i][1], args, adjustSeed(args.seed, N, i), i, N))
        for i in range(N)
    ]
    for p in processes:
        p.start()

    overhead_time = time.time()

    lineno = 0
    num_fields = defaultdict(int)
    try:
        # round-robin across the pipes forever
        while True:
            for pipe in pipes:
                # To avoid pickling (and the associated timing costs), lines
                # are transmitted as strings, not Line objects.
                lines = pipe[0].recv()
                for line in lines:
                    fields = line.split("\t")
                    num_fields[len(fields)] += 1
                    print(line)
                    lineno += 1

                    if (args.log_rate > 0 and lineno % args.log_rate == 0) or lineno <= args.log_first:
                        if args.sample_file:
                            print(line, file=args.sample_file)
                        else:
                            logger.info(f"SAMPLE {lineno}: {line}")
    except BrokenPipeError:  # this is not really an error, just means that the receiving process has ended
        # Python flushes standard streams on exit; redirect remaining output
        # to devnull to avoid another BrokenPipeError at shutdown
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
    finally:
        # Looks like the process that we are piping to is done, let's wrap things up
        for p in processes:
            p.terminate()

        stats['end_time'] = time.time()
        stats['lines_produced'] = f'{lineno:,}'
        stats['num_fields'] = num_fields
        total_time = stats['end_time'] - stats['start_time']
        stats['overhead_time'] = overhead_time - stats['start_time']
        stats['total_time'] = f"{total_time:,.3f} sec"
        stats['yield_rate'] = f"{lineno / total_time:,.2f} lines/sec"
        stats['yield_rate_sans_overhead'] = f"{lineno / (stats['end_time'] - overhead_time):,.2f} lines/sec"
        logger.info('Summary: ' + json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
