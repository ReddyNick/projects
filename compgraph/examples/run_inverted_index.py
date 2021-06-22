import json
from argparse import ArgumentParser

import compgraph.operations as operations
from compgraph.graph import Graph


def inverted_index_graph_from_file(input_file_name: str, doc_column: str = 'doc_id', text_column: str = 'text',
                                   result_column: str = 'tf_idf') -> Graph:
    """Constructs graph which calculates td-idf for every word/document pair"""
    number_of_docs = Graph.graph_from_file(input_file_name, json.loads) \
        .map(operations.Project([doc_column])) \
        .reduce(operations.CountUnique(doc_column, 'n_docs'), [])

    words = Graph.graph_from_file(input_file_name, json.loads) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column))

    frequency = words.sort([doc_column]) \
        .reduce(operations.TermFrequency(text_column, 'freq'), [doc_column]) \
        .sort([text_column])

    presence_in_docs = words.sort([text_column]) \
        .reduce(operations.CountUnique(doc_column, 'presence_in_docs'), [text_column]) \
        .sort([text_column])

    graph = number_of_docs.join(operations.InnerJoiner(), frequency, []) \
        .join(operations.InnerJoiner(), presence_in_docs, [text_column]) \
        .map(operations.Divide(nominator='n_docs', denominator='presence_in_docs', result='fraction')) \
        .map(operations.Log('fraction', 'log')) \
        .map(operations.Product(['freq', 'log'], result_column=result_column)) \
        .map(operations.Project([doc_column, text_column, result_column])) \
        .sort([text_column]) \
        .reduce(operations.TopN(result_column, 3), [text_column])

    return graph


def main() -> None:
    arg_parser = ArgumentParser(prog='Inverted index graph',
                                description='Run inverted index graph to calculate tf-idf')

    arg_parser.add_argument('-i', '--input', required=True, help='Input file path')
    arg_parser.add_argument('-o', '--output', required=True, help='Output file path')
    args = arg_parser.parse_args()

    graph = inverted_index_graph_from_file(args.input)
    result = graph.run()

    with open(args.output, "w") as out:
        for row in result:
            print(row, file=out)


if __name__ == "__main__":
    main()
