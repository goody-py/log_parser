#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import copy
import json
import string
import ConfigParser
import argparse
import logging

from counter import yield_report_row
from file_utils import yield_line_from_file, find_last_log_to_process, get_report_name, is_report_exist


DEFAULT_CONFIG_PATH = './log_parser.cfg'

LOGGING_FORMAT = '[%(asctime)s] %(levelname).1s %(message)s'
LOGGING_DATE_FORMAT = '%Y.%m.%d $H:%M:%S'


config = {
    'REPORT_SIZE': 10,
    'REPORT_DIR': './reports',
    'LOG_DIR': './log',
    'TEMPLATE_PATH': './report.html',
    'LOGGING_LEVEL': 'INFO'
}


def _setup_logger(logging_level, logging_path=None):
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
    """ Parsing and return arguments from sys.argv[1:]
    return: dict - parsed received arguments
    """
    parser = argparse.ArgumentParser(
        description='Tool for parsing nginx logs, log format described in parsers.py . '
                    'All configurations will be prioritized in descending order and merged: '
                    'Passed arguments -> Passed config path -> Default config path -> log_parser.py/config'
    )
    parser.add_argument('-c', '--config-path', help='Configuration file path')
    parser.add_argument('-s', '--report-size', type=int, help='Output report size')
    parser.add_argument('-rd', '--report-dir', help='Path to the report directory')
    parser.add_argument('-ld', '--log-dir', help='Path lo the log directory')
    parser.add_argument('-tp', '--template-path', help='Path to template')
    return vars(parser.parse_args())


def get_dict_with_lower_case_keys(_dict):
    """ Return new dict which keys converted to lower case
    """
    if not _dict:
        return {}
    return {key.lower(): value for key, value in _dict.iteritems()}


def filter_none_dict_values(_dict):
    """ Return new dict, which all values are not None
    """
    if not _dict:
        return {}
    return {key: value for key, value in _dict.iteritems() if value is not None}


def get_config_from_config_file(config_path):
    """ Parsing config from received config file path
    return: dict
    """
    if not config_path:
        return {}
    _config = ConfigParser.ConfigParser()
    _config.read(os.path.abspath(config_path))
    _config = {key: value for key, value in _config.items('log_parser') if value}
    if 'report_size' in _config:
        _config['report_size'] = int(_config['report_size'])
    return _config


def get_result_config_dict(_config):
    """ Merging all config values
    return: dict
    """
    arguments_dict = get_call_arguments()

    passed_config_path = arguments_dict.pop('config_path', None)

    arguments_config_values = filter_none_dict_values(arguments_dict)
    passed_config = filter_none_dict_values(get_config_from_config_file(passed_config_path))
    default_config = filter_none_dict_values(get_config_from_config_file(DEFAULT_CONFIG_PATH))
    __config = get_dict_with_lower_case_keys(copy.copy(_config))

    __config.update(default_config)
    __config.update(passed_config)
    __config.update(arguments_config_values)

    return __config


def main(_config):
    result_config = get_result_config_dict(_config)

    _setup_logger(result_config.get('logging_level'), result_config.get('logging_path'))
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
    report_name = get_report_name(file_to_open)
    report_directory = os.path.abspath(result_config['report_dir'])
    if is_report_exist(report_name, report_directory):
        logging.info('Report exists, report path: {}'.format(os.path.join(report_directory, report_name)))
        sys.exit()

    report_data = yield_report_row(yield_line_from_file(file_to_open), result_config['report_size'])

    report_data = json.dumps([line for line in report_data])

    if not report_data:
        logging.error('Parsed data result is empty. Please make sure, that provided log file is not empty')
        sys.exit()

    with open(report_template_path, 'r') as f:
        data_template = string.Template(f.read())
        with open(os.path.join(report_directory, report_name), 'w') as f2:
            logging.info('Writing report to: {}'.format(os.path.join(report_directory, report_name)))
            f2.write(data_template.safe_substitute(table_json=report_data).decode('utf-8'))


if __name__ == '__main__':
    main(config)
