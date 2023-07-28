import sys
import re
import logging

logger = logging.getLogger(f"sotastream")


def SkipBlanks(lines, fields=[0, 1]):
    """
    Skips lines that are blank in any of the requested fields.
    Also zeroes out the third field if present (to reset docid).
    This is important for training document models, where a blank field can teach the model
    to drop / add sentences.

    :param lines: The data stream
    :param fields: fields to check for blankness
    """
    skipped_prev = False
    for line in lines:
        for fieldno in fields:
            if fieldno >= len(line) or line[fieldno] is None or line[fieldno] == "":
                skipped_prev = True
                break
        else:
            # If we skipped the previous line, we invalidate the current document ID
            if skipped_prev and len(fields) >= 3:
                fields[2] = 0
            skipped_prev = False

            yield line


def BitextFilter(lines, end_range=2):
    """
    Removes all fields up to end_range.

    :param lines: the stream of input lines
    :param end_range: One higher than the last 0-index field number that should be included.
    """
    for line in lines:
        line.fields = line.fields[0:end_range]
        yield line


def MatchFilter(lines, pattern=r'[\=\+\#\@\^\~\<\>]', fields=[0, 1], invert=False):
    for line in lines:
        if len(line) < 2:
            logger.debug(f"MatchFilter: bad line: {line}")
            continue

        if len(fields) != 2:
            raise IndexError("need to specify two field indices for matching")

        f1 = line[fields[0]]
        f2 = line[fields[1]]

        criterion = sorted(re.findall(pattern, f1)) == sorted(re.findall(pattern, f2))
        if (not invert and criterion) or (invert and not criterion):
            yield line


def RegexFilter(lines, pattern, fields=[0, 1], invert=False):
    """
    Removes a line if the pattern is found in one or more fields.
    """
    regex = re.compile(pattern)
    for line in lines:
        if len(line) < len(fields):
            logger.debug(f"RegexFilter: bad line: {line}")
            continue

        founds = [regex.search(line[field]) for field in fields]
        if (not invert and not any(founds)) or (invert and all(founds)):
            yield line
