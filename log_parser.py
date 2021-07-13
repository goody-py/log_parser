#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import gzip
import json
import string
import datetime
import re
import ConfigParser
import argparse
import logging
from array import array
from collections import defaultdict, namedtuple


LOGGING_FORMAT = '[%(asctime)s] %(levelname).1s %(message)s'
LOGGING_DATE_FORMAT = '%Y.%m.%d %H:%M:%S'


FAILURE_PERCENTAGE = 40


REPORT_NAME = 'report-{}.html'
REPORT_DATE_FORMAT = '%Y.%m.%d'


LOG_PREFIX = 'nginx-access-ui'
FILE_DATE_FORMAT = '%Y%m%d'


CONFIG = {
    'REPORT_SIZE': 100,
    'REPORT_DIR': './reports',
    'LOG_DIR': './log',
    'TEMPLATE_PATH': './report.html',
    'LOGGING_LEVEL': 'CRITICAL',
    'DEFAULT_CONFIG_PATH': './log_parser.cfg'

}

# Log filename format 'LOG_PREFIX.log-%Y%m%d', extension can be omitted
FILE_NAME_PATTERN = re.compile(r'(?P<file_name>%s)\S*.log-(?P<date>\d{8})(?P<extension>\.gz)?' % LOG_PREFIX)

# log_format ui_short '$remote_addr $remote_user $http_x_real_ip
# [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent"
# "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" $request_time
LOG_FORMAT_PATTERN = re.compile(
    r'(?P<remote_addr>^\S+)\s*'
    r'\s*(?P<remote_user>\S+)\s*'
    r'\s*(?P<http_x_real_ip>(\S+))\s*'
    r'\s*(?P<time_local>\[.*\])\s*'
    r'\s*\"\S+\s(?P<request>.*?)\s\S+\"\s*'
    r'\s*(?P<status>\S+)\s*'
    r'\s*(?P<body_bytes_sent>\S+)\s*'
    r'\s*\"(?P<http_referrer>.*?)\"\s*'
    r'\s*\"(?P<http_user_agent>.*?)\"\s*'
    r'\s*\"(?P<http_forwarded_for>.*?)\"\s*'
    r'\s*\"(?P<http_x_request_id>.*?)\"\s*'
    r'\s*\"(?P<http_x_rb_user>.*?)\"\s*'
    r'\s*(?P<request_time>\S+)\s*'
)

ParsedFileName = namedtuple('ParsedFileName', ['file_path', 'parsed_date', 'extension'])

# Definitely there are chances to improve performance by using numpy array and np.mean
# Also there are chances to improve memory usage by implementing algorithm below
# https://www.researchgate.net/publication/247974442_The_Remedian_A_Robust_Averaging_Method_for_Large_Data_Sets
def get_median(arr):
    """ Counts array median """
    arr_len = len(arr)
    if arr_len % 2 == 0:
        return sum(arr[(arr_len // 2) - 1:arr_len // 2 + 1]) / 2
    return arr[(arr_len // 2) + 1]


def setup_logger(logging_level, logging_path=None):
    """ Setup logging config for script
    logging_level: str - INFO|DEBUG|ERROR
    logging_path: str - full path to logging file, stdout by default
    """
    logging_numeric_lvl = getattr(logging, logging_level.upper())
    logging.basicConfig(
        filename=logging_path,
        filemode='w',
        format=LOGGING_FORMAT,
        datefmt=LOGGING_DATE_FORMAT,
        level=logging_numeric_lvl
    )


def get_call_arguments():
    """ Parse and return arguments from sys.argv[1:]
    return: dict - parsed received arguments
    """
    parser = argparse.ArgumentParser(
        description='Tool for parsing nginx logs, log format described in parsers.py. '
                    'All configurations will be prioritized in descending order and merged: '
                    'Passed arguments -> Passed config path -> Default config path -> log_parser.py/config'
    )
    parser.add_argument('-c', '--config-path', help='Configuration file path')
    return vars(parser.parse_args())


def get_config_from_config_file(config_path):
    """ Parse config from received config file path
    return: dict
    """
    if not config_path:
        return {}
    config = ConfigParser.ConfigParser()
    config.read(os.path.abspath(config_path))
    config = {key.lower(): value for key, value in config.items('log_parser') if value}
    if 'report_size' in config:
        config['report_size'] = int(config['report_size'])
    return config


def get_result_config_dict(const_config, path_to_config_file):
    """ Merge all config values
    :param: default_config - default config dict
    :param: passed_config_path - config path from called arguments
    return: dict
    """

    parsed_config_from_file = get_config_from_config_file(path_to_config_file)

    if not parsed_config_from_file:
        return {}

    final_config = {key.lower(): value for key, value in const_config.iteritems()}
    final_config.update(parsed_config_from_file)

    return final_config


def parse_log_string(log_string):
    """ Using LOG_FORMAT_PATTERN to extract request and request_time from log string.
    log_string: str - log string from file
    return: dict
    """
    parsed = re.search(LOG_FORMAT_PATTERN, log_string)
    if not parsed:
        logging.error('Can\'t parse log string. Please make sure that log format regexp is fine. '
                      'Provided log string: {}'.format(log_string))
        return None
    parsed_dict = parsed.groupdict()
    return {'request': parsed_dict['request'], 'request_time': float(parsed_dict['request_time'])}


def yield_line_from_file(file_namedtuple):
    """ Yield raw row from received file
    file_namedtuple: ParsedFileName - namedtuple with log file data
    """
    openers = {None: open, 'gz': gzip.open}
    opener = openers.get(file_namedtuple.extension)
    if not opener:
        logging.exception('Can\'t find file opener. Please make sure that log exists and log file is in text/.gz format')
        raise TypeError('Not appropriate file type.\n'
                        'Received file:{0}.\nSupported file types: plain-text/.gz .\n'.format(file_namedtuple.file_path)
                        )
    with opener(file_namedtuple.file_path, 'r') as file:
        for line in file:
            yield line.encode('utf-8')


def find_last_log_to_process(directory_path):
    """ Return log file with max date from received directory
    directory_path: str - path to log directory
    return: None|ParsedFileName
    """
    max_parsed_filename = None
    max_date_filename = None
    log_path = os.path.abspath(directory_path)
    for filename in os.listdir(log_path):
        parsed_filename = re.search(FILE_NAME_PATTERN, filename)
        if not parsed_filename:
            logging.info('Can\'t parse filename. Please make sure that filename matches '
                         'with regexp pattern. Provided filename: {}'.format(filename))
            continue

        parsed_filename = parsed_filename.groupdict()
        try:
            parsed_filename['date'] = datetime.datetime.strptime(parsed_filename['date'], FILE_DATE_FORMAT).date()
        except ValueError as ex:
            logging.exception('Can\'t parse filename date. Please make sure that date format in log name is fine. '
                              'Provided filename: {0} . {1}'.format(filename, ex))
            return None

        if max_parsed_filename and max_parsed_filename['date'] > parsed_filename['date']:
            continue
        max_parsed_filename = parsed_filename
        max_date_filename = filename

    if not max_parsed_filename:
        logging.info('There is no log files to handle. Provided log path: {}'.format(directory_path))
        return None

    return ParsedFileName(
        file_path=os.path.join(log_path, max_date_filename),
        parsed_date=max_parsed_filename['date'],
        extension=max_parsed_filename['extension'] or None
    )


def yield_report_row(raw_row_chain, report_size):
    """ Process and yield report row
    raw_row_chain: iterable - each value is raw string row from log file which will be parsed
    report_size: int - result report size
    return: dict - url report statistic
    """
    def _get_time_sum_value(parsed_data_tuple):
        """ Return time_sum value
        parsed_data_tuple: tuple - (url, data_item)"""
        return parsed_data_tuple[1]['time_sum']

    default_value = lambda: {'query_counter': 0.0, 'requests_time': array('f', []), 'time_max': 0.0, 'time_sum': 0.0}
    parsed_data = defaultdict(default_value)
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

    for url, query_stats in sorted(parsed_data.iteritems(), key=_get_time_sum_value, reverse=True)[:report_size]:
        yield {
            'url': url,
            'count': query_stats['query_counter'],
            'count_perc': round(query_stats['query_counter'] * 100 / all_row_counter, 3),
            'time_avg': round(query_stats['time_sum'] / query_stats['query_counter'], 3),
            'time_max': query_stats['time_max'],
            'time_med': round(get_median(query_stats['requests_time']), 3),
            'time_perc': round(query_stats['time_sum'] * 100 / all_requests_time_counter, 3),
            'time_sum': round(query_stats['time_sum'], 3)
        }


def write_template_report(report_template_path, report_path, report_data):
    """ Substitute report data to report template and create report file
    report_template_path: str - absolute path to report template
    report_path: str - absolute report path
    report_data: str - report json data which converted to str
    """
    with open(report_template_path, 'r') as report_template:
        raw_report = report_template.read()
        if not raw_report:
            logging.error('Report template is empty. Received path: {}'.format(report_template_path))
            raise ValueError('Report template can\'t be empty. Received path: {}'.format(report_template_path))
        filled_template = string.Template(raw_report).safe_substitute(table_json=report_data)
        with open(report_path, 'w') as report:
            logging.info('Writing report to: {}'.format(report_path))
            report.write(filled_template.decode('utf-8'))


def main(config):

    console_arguments = get_call_arguments()

    config_from_file = console_arguments.pop('config_path', None) or config.pop('DEFAULT_CONFIG_PATH', None)
    result_config = get_result_config_dict(config, config_from_file)
    if not result_config:
        logging.error('Result config is empty. Please check default config setup, or '
                      'if you are running app with passed config, make sure that config is configured properly')
        sys.exit()

    setup_logger(result_config.get('logging_level'), result_config.get('logging_path'))
    logging.info('Result config file parameters are: {}'.format(list(result_config.iteritems())))

    report_template_path = os.path.abspath(result_config['template_path'])
    if not os.path.exists(report_template_path):
        logging.info('Can\'t find template for rendering report, received path {}'.format(report_template_path))
        sys.exit()

    file_to_open = find_last_log_to_process(result_config['log_dir'])
    if not file_to_open:
        logging.info('Can\'t find log file to open, received path: {}'.format(result_config['log_dir']))
        sys.exit()

    logging.info('Processing log file: {}'.format(file_to_open.file_path))

    report_name = REPORT_NAME.format(file_to_open.parsed_date.strftime(REPORT_DATE_FORMAT))
    full_report_path = os.path.join(os.path.abspath(result_config['report_dir']), report_name)
    if os.path.exists(full_report_path):
        logging.info('Report exists, report path: {}'.format(full_report_path))
        sys.exit()

    report_data = [line for line in yield_report_row(yield_line_from_file(file_to_open), result_config['report_size'])]

    if not report_data:
        logging.error('Parsed data result is empty. Please make sure, that provided log file is not empty')
        sys.exit()

    write_template_report(
        report_template_path=report_template_path,
        report_path=full_report_path,
        report_data=json.dumps(report_data)
    )


if __name__ == '__main__':
    try:
        main(CONFIG)
    except Exception as ex:
        logging.exception(ex)
