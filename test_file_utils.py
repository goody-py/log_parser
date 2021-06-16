#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest

from file_utils import (yield_line_from_file, ParsedFileName, find_last_log_to_process,
                                   get_report_name, is_report_exist)


class FileUtilsTest(unittest.TestCase):
    pass

if __name__ == '__main__':

    future_fixture = '''1.200.76.128 f032b48fb33e1e692  - [29/Jun/2017:06:12:58 +0300] "GET /api/1/campaigns/?id=1003206 HTTP/1.1" 200 614 "-" "-" "-" "1498705978-4102637017-4707-9891931" "-" 0.141'''
    unittest.main()
