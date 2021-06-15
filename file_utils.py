#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import gzip
import fnmatch
from collections import namedtuple
import logging

from const import LOG_PREFIX
from parsers import parse_file_name, parse_log_string


ParsedFileName = namedtuple('ParsedFileName', ['file_path', 'parsed_date', 'extension'])


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
    unparsed_files = []
    max_parsed_filename = None
    max_date_filename = None
    log_path = os.path.abspath(directory_path)
    for filename in fnmatch.filter(os.listdir(log_path), '{}.*'.format(LOG_PREFIX)):
        if not filename.endswith('.bz'):
            parsed_file_name = parse_file_name(filename)
            if parsed_file_name is None:
                unparsed_files.append(filename)
                continue
            if max_parsed_filename and max_parsed_filename['date'] > parsed_file_name['date']:
                continue
            max_parsed_filename = parsed_file_name
            max_date_filename = filename

    logging.info('Unparsed files in provided log directory: {0}. '
                 'Unparsed files list: {1}'.format(len(unparsed_files), unparsed_files))

    if not max_parsed_filename:
        logging.info('There is no log files to handle. Provided log path: {}'.format(directory_path))
        return None

    return ParsedFileName(
        file_path=os.path.join(os.path.abspath(log_path),max_date_filename),
        parsed_date=max_parsed_filename['date'],
        extension=max_parsed_filename['extension'] or None
    )


def get_report_name(parsed_file_name):
    """ Return report filename
    parsed_file_name: ParsedFileName
    return: str - report name
    """
    if not parsed_file_name:
        return None
    return 'report-{}.html'.format(parsed_file_name.parsed_date.strftime('%Y.%m.%d'))


def is_report_exist(report_name, report_path):
    """ Check if report already exists
    report_name: str - report file name
    report_path: str - path to report directory
    return: bool
    """
    if not report_name:
        logging.exception('Report file name was not provide')
        raise ValueError('Please provide report file name!')
    if not os.path.exists(report_path):
        os.mkdir(report_path)
        logging.info('Report directory was created: {}'.format(report_path))
        return False
    return bool(fnmatch.filter(os.listdir(report_path), report_name))
