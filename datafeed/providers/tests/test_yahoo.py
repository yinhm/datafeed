from __future__ import with_statement

import datetime
import os
import unittest

from datafeed.exchange import *
from datafeed.providers.yahoo import *


class YahooSecurityTest(unittest.TestCase):

    def test_abbr_sha(self):
        s = YahooSecurity(SH(), '600028')
        self.assertEqual(s._abbr, 'SS')

    def test_abbr_she(self):
        s = YahooSecurity(SZ(), '000001')
        self.assertEqual(s._abbr, 'SZ')

    def test_yahoo_id(self):
        s = YahooSecurity(SH(), '600028')
        self.assertEqual(str(s), '600028.SS')

    def test_abbr_to_exchange(self):
        ex = YahooSecurity.get_exchange_from_abbr("SS")
        self.assertEqual(ex, SH())

    def test_ss_abbr(self):
        ret = YahooSecurity.from_string('600028.SS')
        self.assertEqual(ret.exchange, SH())
        self.assertEqual(ret.symbol, '600028')
        self.assertEqual(str(ret), '600028.SS')
    

class YahooReportTest(unittest.TestCase):
    _RAW_DATA = '''"GOOG",533.89,"5/3/2011","4:00pm",-4.67,537.13,542.01,529.63,2081574
"AAPL",348.20,"5/3/2011","4:00pm",+1.92,347.91,349.89,345.62,11198607
"600028.SS",8.58,"5/4/2011","1:47am",-0.10,8.64,8.67,8.55,23045288'''


    def test_yahoo_report(self):
        ret = YahooReport.parse(self._RAW_DATA)
        i = 0
        for r in ret:
            if i == 0:
                self.assertEqual(r.security.exchange, YahooNA())
                self.assertEqual(r.security.symbol, 'GOOG')
                self.assertEqual(str(r.date), "2011-05-03")
                self.assertEqual(r.time.hour, 16)
                self.assertEqual(r.time.minute, 0)
                self.assertEqual(r.price, 533.89)
                self.assertEqual(r.change, -4.67)
                self.assertEqual(r.open, 537.13)
                self.assertEqual(r.high, 542.01)
                self.assertEqual(r.low, 529.63)
                self.assertEqual(r.volume, 2081574)

            if i == 2:
                self.assertEqual(r.security.exchange, SH())
                self.assertEqual(r.security.symbol, '600028')

            i += 1

        self.assertEqual(i, 3)


class YahooDayTest(unittest.TestCase):
    def test_parse_day(self):
        path = os.path.dirname(os.path.realpath(__file__))
        f = open(os.path.join(path, 'yahoo_tables.csv'), 'r')
        data = f.read()
        f.close()

        security = YahooSecurity(YahooNA(), 'GOOG')
        iters = YahooDay.parse(security, data)
        i = 0
        for ohlc in iters:
            if i == 0:
                # 2011-05-03,537.13,542.01,529.63,533.89,2081500,533.89
                self.assertEqual(str(ohlc.date), "2011-05-03")
                self.assertEqual(ohlc.open, 537.13)
                self.assertEqual(ohlc.high, 542.01)
                self.assertEqual(ohlc.low, 529.63)
                self.assertEqual(ohlc.close, 533.89)
                self.assertEqual(ohlc.volume, 2081500)
                self.assertEqual(ohlc.adjclose, 533.89)
            i += 1


class YahooReportFetcherTest(unittest.TestCase):

    def test_init(self):
        f = YahooReportFetcher()
        self.assertEqual(f._base_url, 'http://download.finance.yahoo.com/d/quotes.csv')

    def test_init_with_arguments(self):
        f = YahooReportFetcher(time_out=10, request_size=50)
        self.assertEqual(f._time_out, 10)
        self.assertEqual(f._request_size, 50)
        
    def test_init_with_wrong_arguments(self):
        self.assertRaises(AssertionError,
                          YahooReportFetcher,
                          request_size=200)
        
    def test_fetch(self):
        f = YahooReportFetcher(request_size=2)
        s1 = YahooSecurity(YahooNA(), 'GOOG')
        s2 = YahooSecurity(YahooNA(), 'AAPL')
        s3 = YahooSecurity(SH(), '000001')

        def callback(body):
            qs = YahooReport.parse(body)
            for quote in qs:
                if quote.security == s3:
                    # something must wrong if SSE Composite Index goes down to 100
                    self.assertTrue(quote.price > 100)
        
        f.fetch(s1, s2, s3,
                callback=callback)

class YahooDayFetcherTest(unittest.TestCase):

    def test_init(self):
        f = YahooDayFetcher()
        self.assertEqual(f._base_url, 'http://ichart.finance.yahoo.com/table.csv')
        self.assertEqual(f._time_out, 20)
        self.assertEqual(f._max_clients, 10)

    def test_init_with_wrong_arguments(self):
        self.assertRaises(AssertionError,
                          YahooReportFetcher,
                          max_clients=20)
        
    def test_fetch(self):
        f = YahooDayFetcher()
        s1 = YahooSecurity(YahooNA(), 'GOOG')
        s2 = YahooSecurity(YahooNA(), 'AAPL')

        def callback(security, body):
            iters = YahooDay.parse(security, body)
            i = 0
            for ohlc in iters:
                self.assertTrue(ohlc.security in (s1, s2))
                if i == 0 and ohlc.security == s1:
                    self.assertEqual(str(ohlc.date), "2011-04-28")
                    self.assertEqual(ohlc.open, 538.06)
                    self.assertEqual(ohlc.high, 539.25)
                    self.assertEqual(ohlc.low, 534.08)
                    self.assertEqual(ohlc.close, 537.97)
                    self.assertEqual(ohlc.volume, 2037400.0)
                    
                i += 1

        start_date = datetime.datetime.strptime("2011-04-01", "%Y-%m-%d").date()
        end_date = datetime.datetime.strptime("2011-04-28", "%Y-%m-%d").date()
        f.fetch(s1, s2,
                callback=callback,
                start_date=start_date,
                end_date=end_date)

class YahooNewsFetcherTest(unittest.TestCase):
    def test_fetch(self):
        f = YahooNewsFetcher()
        s1 = YahooSecurity(YahooNA(), 'GOOG')
        s2 = YahooSecurity(YahooNA(), 'AAPL')
        s3 = YahooSecurity(SH(), '000001')

        def callback(security, response):
            self.assertTrue(response.body.startswith('<?xml'))

        f.fetch(s1, callback=callback)



if __name__ == '__main__':
    unittest.main()
