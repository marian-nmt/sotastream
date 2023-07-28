from . import Defaults

from typing import List, Optional


class Line:
    """
    A Line object represents a line containined fields. The string representation
    is typically delimited by tabs, and internally we use fields. The fields can
    represent any parallel corpus. Typically, they are source, target, and metadata.
    """

    # Define slots for efficiency. This avoids the use of a dict
    # for each instance, which is a big memory savings.
    # https://docs.python.org/3/reference/datamodel.html#slots
    # https://stackoverflow.com/questions/472000/usage-of-slots
    __slots__ = "fields"

    def __init__(self, rawLine=None, fields=[]) -> None:
        """
        Initializes a new Line object from rawLine or fields.
        Preference is give to rawLine (if non-None).

        Example usage:

        line = Line("Das ist ein Test.\tThis is a test.")
        line[1]
        -> 'This is a test.'

        If rawLine is not defined, fields will be used.

        :param rawLine: The raw input line, tab-delimited.
        :param fields: A list of fields directly.
        """
        if rawLine is not None:
            self.fields = [field.rstrip("\r\n ") for field in rawLine.split("\t")]
        elif fields is None:
            self.fields = []
        else:
            self.fields = [field for field in fields]

    def __str__(self):
        """
        Only join the first and second fields.
        This assumes a canonical output format of "{source}\t{target}".
        If you want to print metadata or have other semantics for these
        fields, you'll have to roll it yourself.
        """
        return "\t".join(self.fields)

    def __len__(self):
        """The length is the number of non-None fields."""
        return len(self.fields)

    def __getitem__(self, i):
        """Return the ith field."""
        if isinstance(i, tuple):
            return self.fields[i[0] : i[1] : i[2]]
        return self.fields[i]

    def __setitem__(self, i, value):
        """Set the ith field."""
        while i >= len(self.fields):
            self.fields.append("")
        self.fields[i] = value

    def __eq__(self, other):
        return isinstance(other, Line) and self.fields == other.fields

    def __hash__(self):
        """Makes the object hashable."""
        return hash(tuple(self.fields))

    def __copy__(self):
        return Line(fields=self.fields)

    @staticmethod
    def join(lines: List["Line"], separator=Defaults.DOC_SEPARATOR, end_range=2):
        """
        Joins columns of lines together using the specified separator.
        Quits at column end_range - 1.

        Example input: join([Line("a\tb\t1"), Line("d\te\t1")], separator="|", end_range=2)
        Example output: Line("a|d\tb|e")

        :param lines: the list of Line objects to join.
        :param separator: the separator to use.
        :param end_range: the column to stop at.

        :return: a new Line object.
        """
        fields = []
        for i in range(end_range):
            fields.append(separator.join([line[i] for line in lines]))

        return Line(fields=fields)

    def append(self, other: "Line", fields: Optional[List[int]] = None, separator=Defaults.SEPARATOR):
        """
        Append field-wise, on the specified fields.
        If the current Line has fewer fields than the Line being appended,
        it is padded to match.

        :param other: the Line object to append.
        :param fields: the list of fields to append (None means all fields).
        """
        if fields is None:
            fields = list(range(len(other)))

        for i in fields:
            # skip non-existent fields (protects against caller listing too many fields)
            if i >= len(other):
                break
            while i >= len(self):
                self.fields.append("")

            if self[i] == "":
                self[i] = other[i]
            else:
                self[i] += separator + other[i]
