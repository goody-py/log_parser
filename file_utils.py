#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import gzip
import fnmatch
import json
from string import Template
from collections import namedtuple

from const import LOG_PREFIX
from parsers import parse_file_name, parse_log_string


unparsed_files = []


ParsedFileName = namedtuple('ParsedFileName', ['file_path', 'parsed_date', 'extension'])


# TODO fill correct exceptions handlers and add the logging
def yield_line_from_file(file_namedtuple):
    openers = {None: open, 'gz': gzip.open}
    opener = openers.get(file_namedtuple.extension)
    if not opener:
        raise TypeError('Not appropriate file type.\n'
                        'Received file:{0}.\nSupported file types: {1}.\n'.format(file_namedtuple.file_path,
                                                                                  openers.keys())
                        )
    with opener(file_namedtuple.file_path, 'r') as file:
        for line in file:
            yield line.encode('utf-8')


#TODO rename function
def get_file_to_open(directory_path):
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

    if not max_parsed_filename:
        return None

    return ParsedFileName(
        file_path=os.path.join(os.path.abspath(log_path),max_date_filename),
        parsed_date=max_parsed_filename['date'],
        extension=max_parsed_filename['extension'] or None
    )



def get_report_name(parsed_file_name):
    if not parsed_file_name:
        return None
    return 'report-{}.html'.format(parsed_file_name.parsed_date.strftime('%Y.%m.%d'))


def is_report_exist(report_name, report_path):
    if not report_name:
        raise ValueError('Please provide report file name!')
    if not os.path.exists(report_path):
        os.mkdir(report_directory)
        return False
    return bool(fnmatch.filter(os.listdir(report_path), report_name))


if __name__ == '__main__':
    from counter import get_parsed_data
    from log_parser import config
    import sys

    report_template_path = os.path.abspath(config['TEMPLATE_PATH'])
    if not os.path.exists(report_template_path):
        print 'REPORT TEMPLATE DOESN\'T EXIST!'
        sys.exit()
    file_to_open = get_file_to_open(config['LOG_DIR'])
    if not file_to_open:
        print 'There is no log file to open'
        sys.exit()
    rep_name =  get_report_name(file_to_open)
    report_directory = os.path.abspath(config['REPORT_DIR'])
    # print os.path.join(report_directory, rep_name)
    if is_report_exist(rep_name, report_directory):
        print 'REPORT ALREADY EXIST'
        sys.exit()
    data = get_parsed_data(yield_line_from_file(file_to_open), 10)
    final_data = json.dumps([item for item in data])
    if not final_data:
        print 'THERE IS NO DATA TO WRITE!'
        sys.exit()
    with open(report_template_path, 'r') as f:
        data_template = Template(f.read())
        with open(os.path.join(report_directory, rep_name), 'w') as f2:
            f2.write(data_template.safe_substitute(table_json=final_data).encode('utf-8'))

