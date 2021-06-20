#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import os
import gzip
import tempfile
import shutil
import datetime
import random

from file_utils import (yield_line_from_file, ParsedFileName, find_last_log_to_process,
                        get_report_name, is_report_exist, REPORT_NAME, REPORT_DATE_FORMAT)

from parsers import parse_log_string, LOG_PREFIX, FILE_DATE_FORMAT


class FileUtilsTest(unittest.TestCase):
    # Because of the log format we need to add . to prefix
    FILE_PREFIX = LOG_PREFIX + '.'

    @staticmethod
    def generate_random_list_of_unique_dates():
        start_time = datetime.date.today()
        # Since we can have only one report per day - all days must be unique
        r_values = {random.randint(1, 1095) for _ in xrange(10)}
        return [(start_time - datetime.timedelta(days=r_values.pop())) for _ in xrange(len(r_values))]

    @staticmethod
    def get_strf_log_string_date(_datetime):
        return 'log-{}'.format(_datetime.strftime(FILE_DATE_FORMAT))

    @classmethod
    def setUp(cls):
        cls.temp_directory = tempfile.mkdtemp()
        # Initiate max date and create other files
        random_dates = cls.generate_random_list_of_unique_dates()
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

    def test_get_report_name(self):
        tempfile.mkstemp(prefix=self.FILE_PREFIX,
                         suffix=self.get_strf_log_string_date(self.max_date),
                         dir=self.temp_directory)

        self.assertEqual(REPORT_NAME.format(self.max_date.strftime(REPORT_DATE_FORMAT)),
                         get_report_name(find_last_log_to_process(self.temp_directory)))

    def test_is_report_exist_report_exist(self):
        report_file = tempfile.mkstemp(prefix=REPORT_NAME.format(''),
                                       suffix=self.get_strf_log_string_date(self.max_date),
                                       dir=self.temp_directory)

        self.assertTrue(is_report_exist(os.path.basename(report_file[1]), self.temp_directory))

    def test_is_report_exist_report_doesnt_exist(self):
        report_file_name = get_report_name(ParsedFileName(None, self.max_date, None))

        self.assertFalse(is_report_exist(report_file_name, self.temp_directory))


class OpenerTest(unittest.TestCase):
    def test_yield_line_from_file_text_file(self):
        plain_text_fixture_path = os.path.join(os.path.curdir, 'fixtures/lorem_ipsum_text_test_fixture')
        plain_text_iterator = yield_line_from_file(ParsedFileName(plain_text_fixture_path, None, None))
        with open(plain_text_fixture_path, 'r') as f:
            for line in f:
                self.assertEqual(line.encode('utf-8'), next(plain_text_iterator))

    def test_yield_line_from_file_gz_file(self):
        gz_text_fixture_path = os.path.join(os.path.curdir, 'fixtures/lorem_ipsum_text_test_fixture.gz')
        gz_file_iterator = yield_line_from_file(ParsedFileName(gz_text_fixture_path, None, 'gz'))
        with gzip.open(gz_text_fixture_path, 'r') as f:
            for line in f:
                self.assertEqual(line.encode('utf-8'), next(gz_file_iterator))


class ParseLogStringTest(unittest.TestCase):
    LOG_ROW_FIXTURE = '1.200.76.128 f032b48fb33e1e692  - [29/Jun/2017:06:12:58 +0300] ' \
                      '"GET /api/1/campaigns/?id=1003206 HTTP/1.1" 200 614 "-" "-" "-" ' \
                      '"1498705978-4102637017-4707-9891931" "-" 141'

    def test_parse_log_string(self):
        self.assertEqual({'request': '/api/1/campaigns/?id=1003206', 'request_time': float(141)},
                         parse_log_string(self.LOG_ROW_FIXTURE))


file_utils_and_parse_log_stringTestSuite = unittest.TestSuite()
file_utils_and_parse_log_stringTestSuite.addTest(unittest.makeSuite(FileUtilsTest))
file_utils_and_parse_log_stringTestSuite.addTest(unittest.makeSuite(OpenerTest))
file_utils_and_parse_log_stringTestSuite.addTest(unittest.makeSuite(ParseLogStringTest))


if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(file_utils_and_parse_log_stringTestSuite)
