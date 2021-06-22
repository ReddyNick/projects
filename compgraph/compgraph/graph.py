import typing as tp

from . import operations as ops
from .external_sort import ExternalSort


class Graph:
    """Computational graph implementation"""
    def __init__(self, pipeline: ops.Operation) -> None:
        self.pipeline = pipeline

    @staticmethod
    def graph_from_iter(name: str) -> 'Graph':
        """Construct new graph which reads data from row iterator (in form of sequence of Rows
        from 'kwargs' passed to 'run' method) into graph data-flow
        Use ops.ReadIterFactory
        :param name: name of kwarg to use as data source
        """
        return Graph(ops.ReadIterFactory(name))

    @staticmethod
    def graph_from_file(filename: str, parser: tp.Callable[[str], ops.TRow]) -> 'Graph':
        """Construct new graph extended with operation for reading rows from file
        Use ops.Read
        :param filename: filename to read from
        :param parser: parser from string to Row
        """
        return Graph(ops.Read(filename, parser))

    def map(self, mapper: ops.Mapper) -> 'Graph':
        """Construct new graph extended with map operation with particular mapper
        :param mapper: mapper to use
        """
        return Graph(self.AppendOperation(ops.Map(mapper), self.pipeline))

    def reduce(self, reducer: ops.Reducer, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with reduce operation with particular reducer
        :param reducer: reducer to use
        :param keys: keys for grouping
        """
        return Graph(self.AppendOperation(ops.Reduce(reducer, keys), self.pipeline))

    def sort(self, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with sort operation
        :param keys: sorting keys (typical is tuple of strings)
        """
        return Graph(self.AppendOperation(ExternalSort(keys), self.pipeline))

    def join(self, joiner: ops.Joiner, join_graph: 'Graph', keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with join operation with another graph
        :param joiner: join strategy to use
        :param join_graph: other graph to join with
        :param keys: keys for grouping
        """
        return Graph(self.JoinGraphs(ops.Join(joiner, keys), self, join_graph))

    def run(self, **kwargs: tp.Any) -> ops.TRowsIterable:
        """Single method to start execution; data sources passed as kwargs"""
        yield from self.pipeline(**kwargs)

    class AppendOperation(ops.Operation):
        """
        Make composition of two operations
        """
        def __init__(self, op_one: ops.Operation, pipeline: ops.Operation) -> None:
            self.op_one = op_one
            self.pipeline = pipeline

        def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> ops.TRowsGenerator:
            yield from self.op_one(self.pipeline(**kwargs))

    class JoinGraphs(ops.Operation):
        """
        Implements join operation of two graphs' dataflows
        """
        def __init__(self, join: ops.Join, graph_one: 'Graph', graph_two: 'Graph') -> None:
            self.join = join
            self.graph_one = graph_one
            self.graph_two = graph_two

        def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> ops.TRowsGenerator:
            dataflow_one = self.graph_one.run(**kwargs)
            dataflow_two = self.graph_two.run(**kwargs)
            yield from self.join(dataflow_one, dataflow_two)
