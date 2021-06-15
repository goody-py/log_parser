#!/usr/bin/env python
# -*- coding: utf-8 -*-

from array import array
from collections import defaultdict
import logging

from parsers import parse_log_string


FAILURE_PERCENTAGE = 40


round_to_third_digit = lambda number: round(number, 3)


def get_start_parsed_data_value():
    """ Return initial defaultdict value."""
    return {'query_counter': 0.0, 'requests_time': array('f', []), 'time_max': 0.0, 'time_sum': 0.0}


def yield_report_row(raw_row_chain, report_size):
    """ Process and yield report row
    raw_row_chain: iterable - each value is raw string row from log file which will be parsed
    report_size: int - result report size
    return: dict - url report statistic
    """
    def _get_median(arr):
        """ Function to count array median."""
        arr_len = len(arr)
        if arr_len % 2 == 0:
            return sum(arr[(arr_len // 2) - 1:arr_len // 2 + 1]) / 2
        return arr[(arr_len // 2) + 1]

    def _get_time_sum_value(parsed_data_tuple):
        """ Return time_sum value
        parsed_data_tuple: tuple - (url, parsed_data_value)"""
        return parsed_data_tuple[1]['time_sum']

    parsed_data = defaultdict(get_start_parsed_data_value)
    all_row_counter = 0
    empty_or_unparsed_row_counter = 0.0
    all_requests_time_counter = 0

    for row in raw_row_chain:
        parsed_str = parse_log_string(row)
        if not parsed_str:
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

    failure_percentage = empty_or_unparsed_row_counter * 100 / all_row_counter
    logging.info('Failure row parsing percentage is :{}%'.format(failure_percentage))
    if failure_percentage >= FAILURE_PERCENTAGE:
        logging.error('Unparsed rows are above the limit. Please make sure that log file is not corrupted or check '
                      'the log parser regexp. Failure percentage: {}%'.format(failure_percentage))
        raise ValueError('Unparsed row percentage is above the limit,'
                         'you need to check log file or regexp of the row parser}')

    for url, query_stats in iter(
            sorted(parsed_data.iteritems(), key=_get_time_sum_value, reverse=True)[:report_size]):
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
