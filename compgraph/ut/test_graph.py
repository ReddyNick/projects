import dataclasses
import typing as tp

import pytest
from pytest import approx

from compgraph import operations as ops
from compgraph.graph import Graph


@dataclasses.dataclass
class MapCase:
    mapper: ops.Mapper
    data: tp.List[ops.TRow]
    etalon: tp.List[ops.TRow]
    cmp_keys: tp.Tuple[str, ...]


class _Key:
    def __init__(self, *args: str) -> None:
        self._items = sorted(args)

    def __call__(self, d: tp.Mapping[str, tp.Any]) -> tp.Tuple[str, ...]:
        return tuple(str(d.get(key)) for key in self._items)


MAP_CASES = [
    MapCase(
        mapper=ops.Log('arg', 'log'),
        data=[
            {'text': 'a', 'arg': 1},
            {'text': 'b', 'arg': 34},
            {'text': 'c', 'arg': 0.5},
        ],
        etalon=[
            {'text': 'a', 'arg': 1, 'log': 0},
            {'text': 'b', 'arg': 34, 'log': approx(3.526360, abs=0.001)},
            {'text': 'c', 'arg': 0.5, 'log': approx(-0.693147181, abs=0.001)},
        ],
        cmp_keys=("text", "arg", "log")
    ),
    MapCase(
        mapper=ops.Divide(nominator='nom', denominator='denom', result='frac'),
        data=[
            {'nom': 1, 'denom': 3},
            {'nom': 10, 'denom': 5},
        ],
        etalon=[
            {'nom': 1, 'denom': 3, 'frac': approx(1 / 3, 0.001)},
            {'nom': 10, 'denom': 5, 'frac': approx(2, 0.001)},
        ],
        cmp_keys=('nom', 'denom', 'frac')
    ),
    MapCase(
        mapper=ops.WeekHour('time', '%Y%m%dT%H%M%S.%f', 'weekday', 'hour'),
        data=[
            {'time': '20171022T131828.330000'},
            {'time': '20171011T161458.927000'},
            {'time': '20171018T110540.664000'},
            {'time': '20171010T184917.569000'}
        ],
        etalon=[
            {'time': '20171022T131828.330000', 'weekday': 'Sun', 'hour': 13},
            {'time': '20171011T161458.927000', 'weekday': 'Wed', 'hour': 16},
            {'time': '20171018T110540.664000', 'weekday': 'Wed', 'hour': 11},
            {'time': '20171010T184917.569000', 'weekday': 'Tue', 'hour': 18}
        ],
        cmp_keys=('time', 'weekday', 'hour')
    ),
    MapCase(
        mapper=ops.Length('start', 'end', 'length'),
        data=[
            {"start": [37.7457925491035, 55.649487976916134], "end": [37.74583127349615, 55.64946475904435]},
            {"start": [37.796916626393795, 55.71529174223542], "end": [37.796895168721676, 55.71521601174027]},
            {"start": [37.603422570973635, 55.77562338206917], "end": [37.60357637889683, 55.77564806677401]},
            {"start": [37.38734079524875, 55.79634306952357], "end": [37.38734926097095, 55.79626637510955]}
        ],
        etalon=[
            {"start": [37.7457925491035, 55.649487976916134], "end": [37.74583127349615, 55.64946475904435],
             "length": approx(0.003545, 0.001)},
            {"start": [37.796916626393795, 55.71529174223542], "end": [37.796895168721676, 55.71521601174027],
             "length": approx(0.008527, 0.001)},
            {"start": [37.603422570973635, 55.77562338206917], "end": [37.60357637889683, 55.77564806677401],
             "length": approx(0.01000, 1e-3)},
            {"start": [37.38734079524875, 55.79634306952357], "end": [37.38734926097095, 55.79626637510955],
             "length": approx(0.0085444, 1e-3)}
        ],
        cmp_keys=("start", "end", "length")
    ),
]


@pytest.mark.parametrize("case", MAP_CASES)
def test_mapper(case: MapCase) -> None:
    key_func = _Key(*case.cmp_keys)

    result = ops.Map(case.mapper)(iter(case.data))
    assert isinstance(result, tp.Iterator)
    assert sorted(case.etalon, key=key_func) == sorted(result, key=key_func)


@dataclasses.dataclass
class ReduceCase:
    reducer: ops.Reducer
    reducer_keys: tp.Tuple[str, ...]
    data: tp.List[ops.TRow]
    etalon: tp.List[ops.TRow]
    cmp_keys: tp.Tuple[str, ...]


REDUCE_CASES = [
    ReduceCase(
        reducer=ops.CountUnique('doc_id', 'n_docs'),
        reducer_keys=(),
        data=[
            {'doc_id': 1}, {'doc_id': 2}, {'doc_id': 3},
            {'doc_id': 1}, {'doc_id': 2}, {'doc_id': 4}
        ],
        etalon=[
            {'n_docs': 4}
        ],
        cmp_keys=('n_docs',)
    ),
    ReduceCase(
        reducer=ops.CountUnique('doc_id', 'n_docs'),
        reducer_keys=('text',),
        data=[
            {'text': 'hello', 'doc_id': 1},
            {'text': 'hello', 'doc_id': 2},
            {'text': 'hello', 'doc_id': 2},
            {'text': 'hello', 'doc_id': 3},
            {'text': 'cat', 'doc_id': 1},
            {'text': 'dog', 'doc_id': 1},
            {'text': 'shark', 'doc_id': 0},
            {'text': 'shark', 'doc_id': 1},
        ],
        etalon=[
            {'text': 'hello', 'n_docs': 3},
            {'text': 'cat', 'n_docs': 1},
            {'text': 'dog', 'n_docs': 1},
            {'text': 'shark', 'n_docs': 2},
        ],
        cmp_keys=('text', 'n_docs')
    ),
    ReduceCase(
        reducer=ops.Speed('length', 'enter', 'leave', '%Y%m%dT%H%M%S.%f', 'speed'),
        reducer_keys=('weekday', 'hour'),
        data=[
            {'weekday': 'Mon', 'hour': 8, 'length': 0.003545, 'enter': '20171023T081946.239000',
             'leave': '20171023T081946.540000'},
            {'weekday': 'Mon', 'hour': 8, 'length': 0.004088, 'enter': '20171023T084457.580000',
             'leave': '20171023T084457.880000'},
            {'weekday': 'Sun', 'hour': 21, 'length': 0.0085444, 'enter': '20171022T212803.879000',
             'leave': '20171022T212808.579000'},
            {'weekday': 'Thu', 'hour': 15, 'length': 0.01000, 'enter': '20171012T150757.544000',
             'leave': '20171012T150910.061000'},
            {'weekday': 'Wed', 'hour': 11, 'length': 0.008527, 'enter': '20171025T110545.219000',
             'leave': '20171025T110552.555000'},
        ],
        etalon=[
            {'weekday': 'Mon', 'hour': 8, 'speed': approx(45.72179, 1e-3)},
            {'weekday': 'Sun', 'hour': 21, 'speed': approx(6.54464, 1e-3)},
            {'weekday': 'Thu', 'hour': 15, 'speed': approx(0.496435, 1e-3)},
            {'weekday': 'Wed', 'hour': 11, 'speed': approx(4.184460, 1e-3)},
        ],
        cmp_keys=('weekday', 'hour', 'speed')
    )
]


@pytest.mark.parametrize("case", REDUCE_CASES)
def test_reducer(case: ReduceCase) -> None:
    key_func = _Key(*case.cmp_keys)

    result = ops.Reduce(case.reducer, case.reducer_keys)(iter(case.data))
    assert isinstance(result, tp.Iterator)
    assert sorted(case.etalon, key=key_func) == sorted(result, key=key_func)


def test_graph_map():
    data = [
        {'one': 1, 'two': 2},
        {'three': 3, 'four': 3}
    ]

    expected = data

    graph = Graph.graph_from_iter('input').map(ops.DummyMapper())

    assert expected == list(graph.run(input=lambda: iter(data)))


def test_graph_reduce():
    data = [
        {'a': 1, 'b': 1},
        {'a': 1, 'b': 2},
        {'a': 2, 'b': 3},
        {'a': 2, 'b': 4}
    ]
    expected = [
        {'a': 1, 'b': 1},
        {'a': 2, 'b': 3}
    ]

    graph = Graph.graph_from_iter('input').reduce(ops.FirstReducer(), ['a'])
    assert expected == list(graph.run(input=lambda: iter(data)))


def test_graph_sort():
    data = [
        {'a': 2, 'b': 5},
        {'a': 4, 'b': 18},
        {'a': 100, 'b': 0},
        {'a': -1.5, 'b': 5}
    ]
    expected = [
        {'a': -1.5, 'b': 5},
        {'a': 2, 'b': 5},
        {'a': 4, 'b': 18},
        {'a': 100, 'b': 0},
    ]

    graph = Graph.graph_from_iter('input').sort(['a'])
    assert expected == list(graph.run(input=lambda: iter(data)))


def test_graph_join():
    data_1 = [
        {'id': 1, 'speed': 5},
        {'id': 3, 'speed': 10},
        {'id': 122, 'speed': -1}
    ]
    data_2 = [
        {'id': 1, 'cost': 80},
        {'id': 3, 'cost': 90},
        {'id': 122, 'cost': 0}
    ]
    expected = [
        {'id': 1, 'speed': 5, 'cost': 80},
        {'id': 3, 'speed': 10, 'cost': 90},
        {'id': 122, 'speed': -1, 'cost': 0}
    ]

    graph_1 = Graph.graph_from_iter('input_1')
    graph_2 = Graph.graph_from_iter('input_2')

    graph = graph_1.join(ops.InnerJoiner(), graph_2, ['id'])
    assert expected == list(graph.run(input_1=lambda: iter(data_1), input_2=lambda: iter(data_2)))


def test_multiple_call() -> None:
    data_1 = [
        {'a': 1, 'b': 1},
        {'a': 1, 'b': 2},
        {'a': 2, 'b': 3},
        {'a': 2, 'b': 4}
    ]
    expected_1 = [
        {'a': 1, 'b': 1},
        {'a': 2, 'b': 3}
    ]

    data_2 = [
        {'a': 10, 'b': 8},
        {'a': 15, 'b': 9},
        {'a': 15, 'b': 3},
        {'a': 15, 'b': 4}
    ]
    expected_2 = [
        {'a': 10, 'b': 8},
        {'a': 15, 'b': 9}
    ]

    graph = Graph.graph_from_iter('input').reduce(ops.FirstReducer(), ['a'])
    assert expected_1 == list(graph.run(input=lambda: iter(data_1)))
    assert expected_2 == list(graph.run(input=lambda: iter(data_2)))
