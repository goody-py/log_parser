#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import datetime
import logging


# Log file prefix
LOG_PREFIX = 'nginx-access-ui'
FILE_DATE_FORMAT = '%Y%m%d'
# Log filename format 'LOG_PREFIX.log-%Y%m%d', extension can be omitted
FILE_NAME_PATTERN = re.compile(r'(?P<file_name>%s)\S*.log-(?P<date>\d{8}).?(?P<extension>\S*)' % LOG_PREFIX)

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
        parsed_dict['date'] = datetime.datetime.strptime(parsed_dict['date'], FILE_DATE_FORMAT).date()
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
