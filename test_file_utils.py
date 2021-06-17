#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import tempfile
import shutil
import datetime
import random

from file_utils import (yield_line_from_file, ParsedFileName, find_last_log_to_process,
                        get_report_name, is_report_exist)
from parsers import LOG_PREFIX, FILE_DATE_FORMAT


class FindLastLogTest(unittest.TestCase):
    # Because of the log format we need to add . to prefix
    FILE_PREFIX = LOG_PREFIX + '.'

    @staticmethod
    def generate_random_list_of_dates():
        start_time = datetime.date.today()
        # Since we can have only one report per day - all days must be unique
        r_values = {random.randint(1, 1095) for _ in xrange(4)}
        return [(start_time - datetime.timedelta(days=r_values.pop())) for _ in xrange(len(r_values))]

    @staticmethod
    def get_strf_log_string_date(_datetime):
        return 'log-{}'.format(_datetime.strftime(FILE_DATE_FORMAT))

    @classmethod
    def setUp(cls):
        cls.temp_directory = tempfile.mkdtemp()
        # Initiate max date and create other files
        random_dates = cls.generate_random_list_of_dates()
        cls.max_date = random_dates.pop(random_dates.index(max(random_dates)))

        for date in random_dates:
            tempfile.mkstemp(prefix=cls.FILE_PREFIX,
                             suffix=cls.get_strf_log_string_date(date),
                             dir=cls.temp_directory)

    @classmethod
    def tearDown(cls):
        shutil.rmtree(cls.temp_directory)

    def test_find_last_log(self):
        max_date_file = tempfile.mkstemp(prefix=self.FILE_PREFIX,
                                         suffix=self.get_strf_log_string_date(self.max_date),
                                         dir=self.temp_directory)

        self.assertTupleEqual(ParsedFileName(max_date_file[1], self.max_date, None),
                              find_last_log_to_process(self.temp_directory))

    def test_ignore_bz_extension_files(self):
        tempfile.mkstemp(prefix=self.FILE_PREFIX,
                         suffix=self.get_strf_log_string_date(self.max_date) + '.bz',
                         dir=self.temp_directory)

        self.assertIsNone(find_last_log_to_process(self.temp_directory).extension)


if __name__ == '__main__':

    future_fixture = '''1.200.76.128 f032b48fb33e1e692  - [29/Jun/2017:06:12:58 +0300] "GET /api/1/campaigns/?id=1003206 HTTP/1.1" 200 614 "-" "-" "-" "1498705978-4102637017-4707-9891931" "-" 0.141'''
    unittest.main()
