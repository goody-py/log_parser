#!/usr/bin/env python
# -*- coding: utf-8 -*-

from array import array
from collections import defaultdict

from parsers import parse_log_string

FAILURE_PERCENTAGE = 0.4

round_to_third_digit = lambda number: round(number, 3)


def get_start_parsed_data_value():
    return {'query_counter': 0.0, 'requests_time': array('f', []), 'time_max': 0.0, 'time_sum': 0.0}


# TODO rename function, args, move inner functions
def get_parsed_data(raw_row_chain, report_size):
    def _get_median(arr):
        arr_len = len(arr)
        if arr_len % 2 == 0:
            return sum(arr[(arr_len // 2) - 1:arr_len // 2 + 1]) / 2
        return arr[(arr_len // 2) + 1]

    def _get_time_sum_value(parsed_data_item):
        return parsed_data_item[1]['time_sum']

    parsed_data = defaultdict(get_start_parsed_data_value)
    all_row_counter = 0
    empty_or_unparsed_row_counter = 0
    all_requests_time_counter = 0

    for row in raw_row_chain:
        parsed_str = parse_log_string(row)
        if not parsed_str:
            #TODO logging, % failure
            empty_or_unparsed_row_counter += 1
            continue

        data_item = parsed_data[parsed_str['request']]
        request_time = parsed_str['request_time']
        data_item['requests_time'].append(request_time)
        data_item['time_sum'] += request_time
        data_item['time_max'] = request_time if request_time > data_item['time_max'] else data_item['time_max']
        data_item['query_counter'] += 1

        all_row_counter += 1
        all_requests_time_counter += request_time

    # TODO correct Error
    if empty_or_unparsed_row_counter / all_row_counter >= FAILURE_PERCENTAGE:
        raise ValueError('Unparsed row percentage above the limit,'
                         'you need to check log file or regular ex of the row parser}')

    for url, query_stats in iter(
            sorted(parsed_data.iteritems(), key=_get_time_sum_value, reverse=True)[:report_size]):
        # TODO do something with round
        yield {
            'url': url,
            'count': query_stats['query_counter'],
            'count_perc': round_to_third_digit(query_stats['query_counter'] * 100 / all_row_counter),
            'time_avg': round_to_third_digit(query_stats['time_sum'] / query_stats['query_counter']),
            'time_max': query_stats['time_max'],
            'time_med': round_to_third_digit(_get_median(query_stats['requests_time'])),
            'time_perc': round_to_third_digit(query_stats['time_sum'] * 100 / all_requests_time_counter),
            'time_sum': round_to_third_digit(query_stats['time_sum'])
        }
