import json
from argparse import ArgumentParser

import compgraph.operations as operations
from compgraph.graph import Graph


def main() -> None:
    arg_parser = ArgumentParser(prog='Word count graph', description='Run word count graph to calculate word count')

    arg_parser.add_argument('-i', '--input', required=True, help='Input file path')
    arg_parser.add_argument('-o', '--output', required=True, help='Output file path')
    args = arg_parser.parse_args()

    input_stream_name = args.input
    text_column = 'text'
    count_column = 'count'

    graph = Graph.graph_from_file(input_stream_name, json.loads) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .sort([text_column]) \
        .reduce(operations.Count(count_column), [text_column]) \
        .sort([count_column, text_column])

    result = graph.run()
    with open(args.output, "w") as out:
        for row in result:
            print(row, file=out)


if __name__ == "__main__":
    main()
