#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 log_format ui_short '$remote_addr $remote_user $http_x_real_ip
 [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent"
 "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" $request_time
"""
import re
import datetime

from const import LOG_PREFIX


FILE_NAME_PATTERN = re.compile('(?P<file_name>%s).log-(?P<date>\d{8}).?(?P<extension>\S*)' % LOG_PREFIX)


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
    """
        Log extension must be in the format 'LOG_PREFIX.log-%Y%m%d'
        filename:  str - the name of the log file itself e.g. 'LOG_PREFIX.log-20170630.gz'
    """
    parsed = re.search(FILE_NAME_PATTERN, filename)
    if not parsed:
        return None
    parsed_dict = parsed.groupdict()
    try:
        parsed_dict['date'] = datetime.datetime.strptime(parsed_dict['date'], '%Y%m%d')
    except ValueError as ex:
        #TODO logging
        return None

    return parsed_dict


def parse_log_string(log_string):
    parsed = re.search(LOG_FORMAT_PATTERN, log_string)
    if not parsed:
        # TODO logging
        return None
    parsed_dict = parsed.groupdict()
    return {'request': parsed_dict['request'], 'request_time': float(parsed_dict['request_time'])}


if __name__ == '__main__':
    a = '''1.200.76.128 f032b48fb33e1e692  - [29/Jun/2017:06:12:58 +0300] "GET /api/1/campaigns/?id=1003206 HTTP/1.1" 200 614 "-" "-" "-" "1498705978-4102637017-4707-9891931" "-" 0.141'''
    print parse_log_string(a)

