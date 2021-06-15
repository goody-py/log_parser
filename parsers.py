#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import datetime
import logging

from const import LOG_PREFIX

# Log filename format 'LOG_PREFIX.log-%Y%m%d', extension can be omitted
FILE_NAME_PATTERN = re.compile('(?P<file_name>%s).log-(?P<date>\d{8}).?(?P<extension>\S*)' % LOG_PREFIX)

# log_format ui_short '$remote_addr $remote_user $http_x_real_ip
# [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent"
# "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" $request_time
LOG_FORMAT_PATTERN = re.compile(
    '(?P<remote_addr>^\S+)\s*'
    '\s*(?P<remote_user>\S+)\s*'
    '\s*(?P<http_x_real_ip>(\S+))\s*'
    '\s*(?P<time_local>\[.*\])\s*'
    '\s*\"\S+\s(?P<request>.*?)\s\S+\"\s*'
    '\s*(?P<status>\S+)\s*'
    '\s*(?P<body_bytes_sent>\S+)\s*'
    '\s*\"(?P<http_referrer>.*?)\"\s*'
    '\s*\"(?P<http_user_agent>.*?)\"\s*'
    '\s*\"(?P<http_forwarded_for>.*?)\"\s*'
    '\s*\"(?P<http_x_request_id>.*?)\"\s*'
    '\s*\"(?P<http_x_rb_user>.*?)\"\s*'
    '\s*(?P<request_time>\S+)\s*'
)


def parse_file_name(filename):
    """ Using FILE_NAME_PATTERN to parse filename string to dict
    filename:  str - the name of the log file itself e.g. 'LOG_PREFIX.log-20170630.gz'
    return: dict
    """
    parsed = re.search(FILE_NAME_PATTERN, filename)
    if not parsed:
        logging.error('Can\'t parse log filename. Please make sure that file regexp is fine. '
                      'Provided file name: {}'.format(filename))
        return None
    parsed_dict = parsed.groupdict()
    try:
        parsed_dict['date'] = datetime.datetime.strptime(parsed_dict['date'], '%Y%m%d')
    except ValueError as ex:
        logging.exception('Can\'t parse filename date. Please make sure that date format in log name is fine. '
                          'Provided filename: {0} . {1}'.format(filename, ex))
        return None

    return parsed_dict


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


if __name__ == '__main__':
    a = '''1.200.76.128 f032b48fb33e1e692  - [29/Jun/2017:06:12:58 +0300] "GET /api/1/campaigns/?id=1003206 HTTP/1.1" 200 614 "-" "-" "-" "1498705978-4102637017-4707-9891931" "-" 0.141'''
