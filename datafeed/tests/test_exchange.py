from __future__ import with_statement

import unittest

from datetime import datetime
from datafeed.exchange import *

class ExchangeTest(unittest.TestCase):

    def test_NYSE(self):
        nyse = NYSE()
        self.assertEqual(str(nyse), 'NYSE')

    def test_singleton(self):
        lon_1 = LON()
        lon_2 = LON()
        self.assertEqual(lon_1, lon_2)

    def test_security(self):
        stock = Security(SH(), '600123')
        self.assertEqual('SH:600123', str(stock))

    def test_security_init_from_abbr(self):
        stock = Security.from_abbr('SH', '600123')
        self.assertEqual('SH:600123', str(stock))

    def test_shanghai_exchange_pre_open_time(self):
        today = datetime.today()
        sh = SH()
        pre_open_time = SH.pre_open_time(day=today)
        ret = datetime.fromtimestamp(pre_open_time)
        self.assertEqual(ret.hour, 9)
        self.assertEqual(ret.minute, 15)

    def test_shanghai_exchange_open_time(self):
        today = datetime.today()
        sh = SH()
        open_time = SH.open_time(day=today)
        ret = datetime.fromtimestamp(open_time)
        self.assertEqual(ret.hour, 9)
        self.assertEqual(ret.minute, 30)

    def test_shanghai_exchange_open_time(self):
        today = datetime.today()
        sh = SH()
        break_time = SH.break_time(day=today)
        ret = datetime.fromtimestamp(break_time)
        self.assertEqual(ret.hour, 11)
        self.assertEqual(ret.minute, 30)

    def test_shanghai_exchange_open_time(self):
        today = datetime.today()
        sh = SH()
        close_time = SZ.close_time(day=today)
        ret = datetime.fromtimestamp(close_time)
        self.assertEqual(ret.hour, 15)
        self.assertEqual(ret.minute, 0)


if __name__ == '__main__':
    unittest.main()
