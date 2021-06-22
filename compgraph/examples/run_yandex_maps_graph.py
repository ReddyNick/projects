import json
from argparse import ArgumentParser

import compgraph.operations as operations
from compgraph.graph import Graph


def yandex_maps_graph(input_file_name_time: str, input_file_name_length: str,
                      enter_time_column: str = 'enter_time', leave_time_column: str = 'leave_time',
                      edge_id_column: str = 'edge_id', start_coord_column: str = 'start', end_coord_column: str = 'end',
                      weekday_result_column: str = 'weekday', hour_result_column: str = 'hour',
                      speed_result_column: str = 'speed') -> Graph:
    """Constructs graph which measures average speed in km/h depending on the weekday and hour"""
    time_format = "%Y%m%dT%H%M%S.%f"

    time = Graph.graph_from_file(input_file_name_time, json.loads) \
        .map(operations.WeekHour(enter_time_column, time_format,
                                 weekday_result_column, hour_result_column)) \
        .sort([edge_id_column])

    length = Graph.graph_from_file(input_file_name_length, json.loads) \
        .map(operations.Length(start_coord_column, end_coord_column, "length")) \
        .sort([edge_id_column])

    graph = time.join(operations.InnerJoiner(), length, [edge_id_column]) \
        .sort([weekday_result_column, hour_result_column]) \
        .reduce(operations.Speed("length", enter_time_column, leave_time_column, time_format, speed_result_column),
                [weekday_result_column, hour_result_column])

    return graph


def main() -> None:
    arg_parser = ArgumentParser(prog='Inverted index graph', description='Run inverted index graph to calculate tf-idf')

    arg_parser.add_argument('-iT', '--input_time', required=True, help='Input time file path')
    arg_parser.add_argument('-iL', '--input_length', required=True, help='Input length file path')
    arg_parser.add_argument('-o', '--output', required=True, help='Output file path')
    args = arg_parser.parse_args()

    graph = yandex_maps_graph(args.input_time, args.input_length)

    result = graph.run()
    with open(args.output, "w") as out:
        for row in result:
            print(row, file=out)


if __name__ == "__main__":
    main()
