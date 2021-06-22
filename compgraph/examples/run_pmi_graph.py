import json
from argparse import ArgumentParser

import compgraph.operations as operations
from compgraph.graph import Graph


def pmi_graph_from_file(input_file_name: str, doc_column: str = 'doc_id', text_column: str = 'text',
                        result_column: str = 'pmi') -> Graph:
    """Constructs graph which gives for every document the top 10 words ranked by pointwise mutual information"""
    words = Graph.graph_from_file(input_file_name, json.loads) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .map(operations.Filter(lambda row: len(row[text_column]) > 4)) \
        .sort([doc_column, text_column])

    filtered = words.sort([doc_column, text_column]) \
        .reduce(operations.Count('count'), [doc_column, text_column]) \
        .map(operations.Filter(lambda row: row['count'] >= 2)) \
        .map(operations.Project([doc_column, text_column])) \

    filtered = filtered.join(operations.InnerJoiner(), words, [doc_column, text_column]) \

    frequency = filtered.reduce(operations.TermFrequency(text_column, 'freq'), [doc_column])\
        .sort([text_column])

    frequency_all = filtered.reduce(operations.TermFrequency(text_column, 'freq_all'), []) \
        .sort([text_column])

    graph = frequency.join(operations.InnerJoiner(), frequency_all, [text_column])\
        .map(operations.Divide(nominator='freq', denominator='freq_all', result='fraq'))\
        .map(operations.Log(arg='fraq', result=result_column)) \
        .map(operations.Project([doc_column, text_column, result_column])) \
        .sort([doc_column, result_column, text_column]) \
        .reduce(operations.TopN(result_column, 10), [doc_column])

    return graph


def main() -> None:
    arg_parser = ArgumentParser(prog='Inverted index graph', description='Run inverted index graph to calculate tf-idf')

    arg_parser.add_argument('-i', '--input', required=True, help='Input file path')
    arg_parser.add_argument('-o', '--output', required=True, help='Output file path')
    args = arg_parser.parse_args()

    graph = pmi_graph_from_file(args.input)
    result = graph.run()

    with open(args.output, "w") as out:
        for row in result:
            print(row, file=out)


if __name__ == "__main__":
    main()
