from . import Graph, operations


def word_count_graph(input_stream_name: str, text_column: str = 'text', count_column: str = 'count') -> Graph:
    """Construct graph  which counts words in text_column of all rows passed"""
    return Graph.graph_from_iter(input_stream_name) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .sort([text_column]) \
        .reduce(operations.Count(count_column), [text_column]) \
        .sort([count_column, text_column])


def inverted_index_graph(input_stream_name: str, doc_column: str = 'doc_id', text_column: str = 'text',
                         result_column: str = 'tf_idf') -> Graph:
    """Constructs graph which calculates td-idf for every word/document pair"""
    number_of_docs = Graph.graph_from_iter(input_stream_name) \
        .map(operations.Project([doc_column])) \
        .reduce(operations.CountUnique(doc_column, 'n_docs'), [])

    words = Graph.graph_from_iter(input_stream_name) \
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


def pmi_graph(input_stream_name: str, doc_column: str = 'doc_id', text_column: str = 'text',
              result_column: str = 'pmi') -> Graph:
    """Constructs graph which gives for every document the top 10 words ranked by pointwise mutual information"""
    words = Graph.graph_from_iter(input_stream_name) \
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


def yandex_maps_graph(input_stream_name_time: str, input_stream_name_length: str,
                      enter_time_column: str = 'enter_time', leave_time_column: str = 'leave_time',
                      edge_id_column: str = 'edge_id', start_coord_column: str = 'start', end_coord_column: str = 'end',
                      weekday_result_column: str = 'weekday', hour_result_column: str = 'hour',
                      speed_result_column: str = 'speed') -> Graph:
    """Construct graph which measures average speed in km/h depending on the weekday and hour"""
    time_format = "%Y%m%dT%H%M%S.%f"

    time = Graph.graph_from_iter(input_stream_name_time) \
        .map(operations.WeekHour(enter_time_column, time_format,
                                 weekday_result_column, hour_result_column)) \
        .sort([edge_id_column])

    length = Graph.graph_from_iter(input_stream_name_length) \
        .map(operations.Length(start_coord_column, end_coord_column, "length")) \
        .sort([edge_id_column])

    graph = time.join(operations.InnerJoiner(), length, [edge_id_column]) \
        .sort([weekday_result_column, hour_result_column]) \
        .reduce(operations.Speed("length", enter_time_column, leave_time_column, time_format, speed_result_column),
                [weekday_result_column, hour_result_column])

    return graph
