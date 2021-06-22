import copy
import heapq
import re
import typing as tp

from abc import abstractmethod, ABC
from datetime import datetime
from itertools import groupby
from math import radians, cos, sin, asin, sqrt, log


TRow = tp.Dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]


class Operation(ABC):
    @abstractmethod
    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        pass


class Read(Operation):
    def __init__(self, filename: str, parser: tp.Callable[[str], TRow]) -> None:
        self.filename = filename
        self.parser = parser

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        with open(self.filename) as f:
            for line in f:
                yield self.parser(line)


class ReadIterFactory(Operation):
    def __init__(self, name: str) -> None:
        self.name = name

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for row in kwargs[self.name]():
            yield row


# Operations


class Mapper(ABC):
    """Base class for mappers"""
    @abstractmethod
    def __call__(self, row: TRow) -> TRowsGenerator:
        """
        :param row: one table row
        """
        pass


class Map(Operation):
    def __init__(self, mapper: Mapper) -> None:
        self.mapper = mapper

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for row in rows:
            yield from self.mapper(row)


class Reducer(ABC):
    """Base class for reducers"""
    @abstractmethod
    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        """
        :param rows: table rows
        """
        pass


class Reduce(Operation):
    def __init__(self, reducer: Reducer, keys: tp.Sequence[str]) -> None:
        self.reducer = reducer
        self.keys = keys

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        if not self.keys:
            yield from self.reducer((), rows)
            return

        for key, group in groupby(rows,
                                  key=lambda row: tuple(row[col] for col in self.keys)):
            yield from self.reducer(tuple(self.keys), group)


class Joiner(ABC):
    """Base class for joiners"""
    def __init__(self, suffix_a: str = '_1', suffix_b: str = '_2') -> None:
        self._a_suffix = suffix_a
        self._b_suffix = suffix_b

    @abstractmethod
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        """
        :param keys: join keys
        :param rows_a: left table rows
        :param rows_b: right table rows
        """
        pass

    @staticmethod
    def separate_columns(row_a: TRow, row_b: TRow, keys: tp.Sequence[str]) -> tp.Tuple[tp.Any, ...]:
        set_a = set(row_a.keys())
        set_b = set(row_b.keys())
        set_keys = set(keys)

        col_from_right = set_b - set_a & set_b
        common_col = set_a & set_b - set_keys
        col_from_left = set_a - common_col

        return col_from_left, col_from_right, common_col

    def join_row_list(self, row_a: TRow, lst_b: tp.List[TRow],
                      col_from_left: tp.Set[str], col_from_right: tp.Set[str],
                      common_col: tp.Set[str]) -> TRowsGenerator:
        for row_b in lst_b:
            new_row = {}
            for col in col_from_left:
                new_row[col] = row_a[col]
            for col in col_from_right:
                new_row[col] = row_b[col]
            for col in common_col:
                new_row[col + self._a_suffix] = row_a[col]
                new_row[col + self._b_suffix] = row_b[col]

            yield new_row

    def join_both(self, keys: tp.Sequence[str], row: TRow, rows_a: TRowsIterable,
                  list_b: tp.List[TRow]) -> TRowsGenerator:
        columns = self.separate_columns(row, list_b[0], keys)
        yield from self.join_row_list(row, list_b, *columns)

        for row in rows_a:
            yield from self.join_row_list(row, list_b, *columns)


class Join(Operation):
    def __init__(self, joiner: Joiner, keys: tp.Sequence[str]):
        self.keys = keys
        self.joiner = joiner

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        rows_a = rows
        rows_b = args[0]
        groupby_a = groupby(rows_a, key=lambda row: tuple(row[col] for col in self.keys))
        groupby_b = groupby(rows_b, key=lambda row: tuple(row[col] for col in self.keys))

        group_one = next(groupby_a, None)
        group_two = next(groupby_b, None)

        while group_one and group_two:
            if group_one[0] < group_two[0]:
                yield from self.joiner(self.keys, group_one[1], iter([]))
                group_one = next(groupby_a, None)
            elif group_two[0] < group_one[0]:
                yield from self.joiner(self.keys, iter([]), group_two[1])
                group_two = next(groupby_b, None)
            else:
                yield from self.joiner(self.keys, group_one[1], group_two[1])
                group_one = next(groupby_a, None)
                group_two = next(groupby_b, None)

        while group_one:
            yield from self.joiner(self.keys, group_one[1], iter([]))
            group_one = next(groupby_a, None)

        while group_two:
            yield from self.joiner(self.keys, iter([]), group_two[1])
            group_two = next(groupby_b, None)


# Dummy operators


class DummyMapper(Mapper):
    """Yield exactly the row passed"""
    def __call__(self, row: TRow) -> TRowsGenerator:
        yield row


class FirstReducer(Reducer):
    """Yield only first row from passed ones"""
    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        for row in rows:
            yield row
            break


# Mappers


class Divide(Mapper):
    """
    Divide one column by another
    """
    def __init__(self, *, nominator: str, denominator: str, result: str) -> None:
        """
        :param result: result column name
        """
        self.nominator = nominator
        self.denominator = denominator
        self.result = result

    def __call__(self, row: TRow) -> TRowsGenerator:
        assert row[self.denominator] != 0
        row[self.result] = row[self.nominator] / row[self.denominator]
        yield row


class Log(Mapper):
    """
    Get logarithm of a column
    """
    def __init__(self, arg: str, result: str) -> None:
        """
        :param arg: column to log
        :param result: result column
        """
        self.arg = arg
        self.result = result

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.result] = log(row[self.arg])
        yield row


class FilterPunctuation(Mapper):
    """Left only non-punctuation symbols"""
    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.column] = re.sub(r"[^\s\w]|_", "", row[self.column])
        yield row


class LowerCase(Mapper):
    """Replace column value with value in lower case"""
    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    @staticmethod
    def _lower_case(txt: str) -> str:
        return txt.lower()

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.column] = self._lower_case(row[self.column])
        yield row


class Split(Mapper):
    """Splits row on multiple rows by separator"""
    def __init__(self, column: str, separator: tp.Optional[str] = None) -> None:
        """
        :param column: name of column to split
        :param separator: string to separate by
        """
        self.column = column
        self.separator = separator
        if self.separator is None:
            self.separator = "\\s+"

    def __call__(self, row: TRow) -> TRowsGenerator:
        if self.separator == "\\s+":
            s = row[self.column] + ' '
        else:
            s = row[self.column] + self.separator

        start = 0
        for match in re.finditer(str(self.separator), s):
            new_row = copy.copy(row)
            new_row[self.column] = s[start:match.start()]
            yield new_row
            start = match.end()


class Product(Mapper):
    """Calculates product of multiple columns"""
    def __init__(self, columns: tp.Sequence[str], result_column: str = 'product') -> None:
        """
        :param columns: column names to product
        :param result_column: column name to save product in
        """
        self.columns = columns
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        prod = 1
        for col in self.columns:
            prod *= row[col]
        row[self.result_column] = prod
        yield row


class Filter(Mapper):
    """Remove records that don't satisfy some condition"""
    def __init__(self, condition: tp.Callable[[TRow], bool]) -> None:
        """
        :param condition: if condition is not true - remove record
        """
        self.condition = condition

    def __call__(self, row: TRow) -> TRowsGenerator:
        if self.condition(row):
            yield row


class Project(Mapper):
    """Leave only mentioned columns"""
    def __init__(self, columns: tp.Sequence[str]) -> None:
        """
        :param columns: names of columns
        """
        self.columns = columns

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield {col: row[col] for col in self.columns}


class WeekHour(Mapper):
    """
    Extract weekday and hour from time using given time format to parse
    """
    def __init__(self, time: str, format: str, weekday_result: str, hour_result: str) -> None:
        """
        :param time: time to parse
        :param format:
        :param weekday_result: result columns for weekday
        :param hour_result: result columns for hour
        """
        self.time = time
        self.format = format
        self.weekday_result = weekday_result
        self.hour_result = hour_result
        self.weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

    def __call__(self, row: TRow) -> TRowsGenerator:
        try:
            dt = datetime.strptime(row[self.time], self.format)
        except ValueError:
            dt = datetime.strptime(row[self.time], "%Y%m%dT%H%M%S")

        row[self.weekday_result] = self.weekdays[dt.weekday()]
        row[self.hour_result] = dt.hour
        yield row


class Length(Mapper):
    """
    Use haversine formula to calculate length between to points on earth
    """
    def __init__(self, start: str, end: str, result: str) -> None:
        """
        :param start: column name of [lon, lat] of start point
        :param end: column name of [lon, lat] of end point
        :param result: result column name
        """
        self.start = start
        self.end = end
        self.result = result

    def __call__(self, row: TRow) -> TRowsGenerator:
        if self.result in row:
            yield row
            return

        row[self.result] = self.haversine(row)
        yield row

    def haversine(self, row: TRow) -> float:
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        """
        lon1, lat1 = row[self.start]
        lon2, lat2 = row[self.end]

        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        r = 6371.0  # Radius of earth in kilometers
        return c * r


# Reducers


class Speed(Reducer):
    """
    Calculate avarage speed for specific weekday and hour
    """
    def __init__(self, length_col: str, enter_col: str, leave_col: str, time_format: str, result: str) -> None:
        """
        :param length_col:  length of the road
        :param enter_col:   time of entering the road
        :param leave_col:   time of leaving the road
        :param time_format: time format
        :param result:      result column name
        """
        self.length_col = length_col
        self.enter_col = enter_col
        self.leave_col = leave_col
        self.time_format = time_format
        self.result = result

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        length_total: float = 0
        time_total: float = 0

        new_row: TRow = {}
        for row in rows:
            if not new_row:
                new_row = {col: row[col] for col in group_key}

            try:
                dt_1 = datetime.strptime(row[self.enter_col], self.time_format)
            except ValueError:
                dt_1 = datetime.strptime(row[self.enter_col], "%Y%m%dT%H%M%S")

            try:
                dt_2 = datetime.strptime(row[self.leave_col], self.time_format)
            except ValueError:
                dt_2 = datetime.strptime(row[self.leave_col], "%Y%m%dT%H%M%S")

            td = dt_2 - dt_1

            time_total += (td.seconds + td.microseconds * 10**(-6)) / 3600
            length_total += row[self.length_col]

        new_row[self.result] = length_total / time_total
        yield new_row


class CountUnique(Reducer):
    """Count the number of unique values of specific column"""
    def __init__(self, column: str, result_column: str) -> None:
        """
        :param column: name of column from where to count unique values
        :param result_column: result column name
        """
        self.column = column
        self.result_column = result_column

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        values_set = set()
        new_row: TRow = {}
        for row in rows:
            if not new_row and group_key:
                new_row = {col: row[col] for col in group_key}

            if row[self.column] in values_set:
                continue
            values_set.add(row[self.column])

        new_row[self.result_column] = len(values_set)

        yield new_row


class TopN(Reducer):
    """Calculate top N by value"""
    def __init__(self, column: str, n: int) -> None:
        """
        :param column: column name to get top by
        :param n: number of top values to extract
        """
        self.column_max = column
        self.n = n

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        heap: tp.List[tp.Tuple[str, int]] = []
        topn: tp.List[TRow] = []
        for row in rows:
            if len(heap) < self.n:
                heapq.heappush(heap, (row[self.column_max], len(heap)))
                topn.append(row)
                continue
            if row[self.column_max] > heap[0][0]:
                replace_idx = heap[0][1]
                heapq.heapreplace(heap, (row[self.column_max], replace_idx))
                topn[replace_idx] = row

        topn.sort(key=lambda row: row[self.column_max], reverse=True)
        return (row for row in topn)


class TermFrequency(Reducer):
    """Calculate frequency of values in column"""
    def __init__(self, words_column: str, result_column: str = 'tf') -> None:
        """
        :param words_column: name for column with words
        :param result_column: name for result column
        """
        self.words_column = words_column
        self.result_column = result_column

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        n_words = 0
        counter: tp.Dict[tp.Any, int] = {}
        new_row: TRow = {}

        for row in rows:
            if not new_row:
                new_row = {col: row[col] for col in group_key}

            if row[self.words_column] not in counter:
                counter[row[self.words_column]] = 1
            else:
                counter[row[self.words_column]] += 1

            n_words += 1

        for key, val in counter.items():
            tf = val / n_words
            row = copy.copy(new_row)
            row[self.words_column] = key
            row[self.result_column] = tf

            yield row


class Count(Reducer):
    """
    Counts records by key
    Example for group_key=('a',) and column='d'
        {'a': 1, 'b': 5, 'c': 2}
        {'a': 1, 'b': 6, 'c': 1}
        =>
        {'a': 1, 'd': 2}
    """
    def __init__(self, column: str) -> None:
        """
        :param column: name for result column
        """
        self.column = column

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        count = 0
        new_row: TRow = {}
        for row in rows:
            if not new_row:
                new_row = {col: row[col] for col in group_key}

            count += 1

        new_row[self.column] = count
        yield new_row


class Sum(Reducer):
    """
    Sum values aggregated by key
    Example for key=('a',) and column='b'
        {'a': 1, 'b': 2, 'c': 4}
        {'a': 1, 'b': 3, 'c': 5}
        =>
        {'a': 1, 'b': 5}
    """
    def __init__(self, column: str) -> None:
        """
        :param column: name for sum column
        """
        self.column = column

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        s = 0
        new_row: TRow = {}
        for row in rows:
            if not new_row:
                new_row = {col: row[col] for col in group_key}

            s += row[self.column]

        new_row[self.column] = s
        yield new_row


# Joiners


class InnerJoiner(Joiner):
    """Join with inner strategy"""
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        list_b = list(rows_b)
        if not list_b:
            return

        row = next(iter(rows_a), None)
        if row is None:
            return

        yield from self.join_both(keys, row, rows_a, list_b)


class OuterJoiner(Joiner):
    """Join with outer strategy"""
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable,
                 rows_b: TRowsIterable) -> TRowsGenerator:
        list_b = list(rows_b)
        if not list_b:
            yield from rows_a
            return

        row = next(iter(rows_a), None)
        if row is None:
            yield from list_b
            return

        yield from self.join_both(keys, row, rows_a, list_b)


class LeftJoiner(Joiner):
    """Join with left strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable,
                 rows_b: TRowsIterable) -> TRowsGenerator:
        list_b = list(rows_b)
        if not list_b:
            yield from rows_a
            return

        row = next(iter(rows_a), None)
        if row is None:
            return

        yield from self.join_both(keys, row, rows_a, list_b)


class RightJoiner(Joiner):
    """Join with right strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable,
                 rows_b: TRowsIterable) -> TRowsGenerator:
        list_a = list(rows_a)
        if not list_a:
            yield from rows_b

        row = next(iter(rows_b), None)
        if row is None:
            return

        yield from self.join_both(keys, row, rows_b, list_a)
