#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import with_statement

import os
import unittest

from datetime import datetime, date
from datafeed.exchange import *
from datafeed.providers.http_fetcher import *
from datafeed.providers.google import *


class GoogleSecurityTest(unittest.TestCase):

    def test_abbr_sha(self):
        s = GoogleSecurity(SH(), '600028')
        self.assertEqual(s._abbr, 'SHA')

    def test_abbr_she(self):
        s = GoogleSecurity(SZ(), '000001')
        self.assertEqual(s._abbr, 'SHE')

    def test_abbr_hgk(self):
        s = GoogleSecurity(HK(), '000001')
        self.assertEqual(str(s._abbr), 'HGK')

    def test_google_id(self):
        s = GoogleSecurity(SH(), '600028')
        self.assertEqual(str(s), 'SHA:600028')

    def test_abbr_to_exchange(self):
        ex = GoogleSecurity.get_exchange_from_abbr("SHA")
        self.assertEqual(ex, SH())

    def test_ss_abbr(self):
        ret = GoogleSecurity.from_string('SHA:600028')
        self.assertEqual(ret.exchange, SH())
        self.assertEqual(ret.symbol, '600028')
        self.assertEqual(str(ret), 'SHA:600028')
    
    def test_zip_slice(self):
        ret = [r for r in zip_slice(3, 'ABCED')]
        self.assertEqual(ret, [('A', 'B', 'C'), ('E', 'D', None)])


class GoogleReportTest(unittest.TestCase):
    _RAW_DATA = '// [ { "id": "7521596" ,"t" : "000001" ,"e" : "SHA" ,"l" : "2,925.53" ,"l_cur" : "CN¥2,925.53" ,"s": "0" ,"ltt":"3:00PM CST" ,"lt" : "Apr 27, 3:00PM CST" ,"c" : "-13.46" ,"cp" : "-0.46" ,"ccol" : "chr" ,"eo" : "" ,"delay": "" ,"op" : "2,946.33" ,"hi" : "2,961.13" ,"lo" : "2,907.66" ,"vo" : "105.49M" ,"avvo" : "" ,"hi52" : "3,478.01" ,"lo52" : "1,844.09" ,"mc" : "" ,"pe" : "" ,"fwpe" : "" ,"beta" : "" ,"eps" : "" ,"name" : "SSE Composite Index" ,"type" : "Company" } ,{ "id": "697073" ,"t" : "600028" ,"e" : "SHA" ,"l" : "8.64" ,"l_cur" : "CN¥8.64" ,"s": "0" ,"ltt":"3:00PM CST" ,"lt" : "Apr 29, 3:00PM CST" ,"c" : "+0.12" ,"cp" : "1.41" ,"ccol" : "chg" ,"eo" : "" ,"delay": "" ,"op" : "8.57" ,"hi" : "8.66" ,"lo" : "8.53" ,"vo" : "42.28M" ,"avvo" : "" ,"hi52" : "10.09" ,"lo52" : "7.67" ,"mc" : "749.11B" ,"pe" : "10.70" ,"fwpe" : "" ,"beta" : "" ,"eps" : "0.81" ,"name" : "China Petroleum \x26 Chemical Corporation" ,"type" : "Company" } ,{ "id": "694653" ,"t" : "GOOG" ,"e" : "NASDAQ" ,"l" : "532.82" ,"l_cur" : "532.82" ,"s": "1" ,"ltt":"4:00PM EDT" ,"lt" : "Apr 26, 4:00PM EDT" ,"c" : "+7.77" ,"cp" : "1.48" ,"ccol" : "chg" ,"el": "535.97" ,"el_cur": "535.97" ,"elt" : "Apr 27, 4:15AM EDT" ,"ec" : "+3.15" ,"ecp" : "0.59" ,"eccol" : "chg" ,"div" : "" ,"yld" : "" ,"eo" : "" ,"delay": "" ,"op" : "526.52" ,"hi" : "537.44" ,"lo" : "525.21" ,"vo" : "100.00" ,"avvo" : "2.80M" ,"hi52" : "642.96" ,"lo52" : "433.63" ,"mc" : "171.31B" ,"pe" : "19.53" ,"fwpe" : "" ,"beta" : "1.19" ,"eps" : "27.28" ,"name" : "Google Inc." ,"type" : "Company" } ]'


    def test_currenct_to_float(self):
        self.assertEqual(currency2float("10.08"), 10.08)
        self.assertEqual(currency2float("12,313.66"), 12313.66)
        self.assertEqual(currency2float("102.5M"), 102500000)
    
    def test_google_report(self):
        ret = GoogleReport.parse(self._RAW_DATA)
        i = 0
        for r in ret:
            if i == 0:
                self.assertEqual(r.security.exchange, SH())
                self.assertEqual(r.security.symbol, '000001')
                self.assertEqual(r.price, 2925.53)
                self.assertEqual(r.open, 2946.33)
                self.assertEqual(r.high, 2961.13)
                self.assertEqual(r.low, 2907.66)
                self.assertEqual(r.change, -13.46)

                diff = r.preclose - 2938.99
                self.assertTrue(abs(diff) < 0.000001)
                self.assertTrue(isinstance(r.time, datetime))
                self.assertEqual(r.time.hour, 15)
            if i == 2:
                self.assertEqual(r.security.exchange, NASDAQ())
                self.assertEqual(r.security.symbol, 'GOOG')
                self.assertTrue(r.time.hour, 16)

                self.assertEqual(r['el'], "535.97")
                

            i += 1

    def test_google_report_parse_with_excption(self):
        data = '// [ { "id": "694653" ,"t" : "GOOG" ,"e" : "NASDAQ" ,"l" : "520.90" ,"l_cur" : "520.90" ,"s": "0" ,"ltt":"4:00PM EDT" ,"lt" : "May 27, 4:00PM EDT" ,"c" : "+2.77" ,"cp" : "0.53" ,"ccol" : "chg" ,"eo" : "" ,"delay": "" ,"op" : "518.48" ,"hi" : "521.79" ,"lo" : "516.30" ,"vo" : "1.75M" ,"avvo" : "2.91M" ,"hi52" : "642.96" ,"lo52" : "433.63" ,"mc" : "167.86B" ,"pe" : "20.23" ,"fwpe" : "" ,"beta" : "1.17" ,"eps" : "25.75" ,"name" : "Google Inc." ,"type" : "Company" } ,{ "id": "697227" ,"t" : "FRCMQ" ,"e" : "PINK" ,"l" : "0.0045" ,"l_cur" : "0.00" ,"s": "0" ,"ltt":"2:13PM EST" ,"lt" : "Jan 24, 2:13PM EST" ,"c" : "0.0000" ,"cp" : "0.00" ,"ccol" : "chb" ,"eo" : "" ,"delay": "15" ,"op" : "" ,"hi" : "" ,"lo" : "" ,"vo" : "0.00" ,"avvo" : "1.17M" ,"hi52" : "0.14" ,"lo52" : "0.00" ,"mc" : "404,839.00" ,"pe" : "0.00" ,"fwpe" : "" ,"beta" : "1.30" ,"eps" : "7.57" ,"name" : "Fairpoint Communications, Inc." ,"type" : "Company" } ,{ "id": "5521731" ,"t" : "APPL" ,"e" : "PINK" ,"l" : "0.0000" ,"l_cur" : "0.00" ,"s": "0" ,"ltt":"" ,"lt" : "" ,"c" : "" ,"cp" : "" ,"ccol" : "" ,"eo" : "" ,"delay": "15" ,"op" : "" ,"hi" : "" ,"lo" : "" ,"vo" : "0.00" ,"avvo" : "" ,"hi52" : "" ,"lo52" : "" ,"mc" : "" ,"pe" : "" ,"fwpe" : "" ,"beta" : "" ,"eps" : "" ,"name" : "APPELL PETE CORP" ,"type" : "Company" } ]'

        iterable = GoogleReport.parse(data)

        i = 0
        while 1:
            try:
                i += 1
                r = iterable.next()
            except ValueError:
                continue
            except KeyError:
                continue
            except StopIteration:
                break

            if i == 1:
                self.assertEqual(r.security.symbol, 'GOOG')
            if i == 3:
                self.assertEqual(r.security.symbol, 'APPL')


class GoogleDayTest(unittest.TestCase):
    def test_parse_day(self):
        path = os.path.dirname(os.path.realpath(__file__))
        f = open(os.path.join(path, 'google_data.csv'), 'r')
        data = f.read()
        f.close()

        security = GoogleSecurity(NASDAQ(), 'GOOG')
        iters = GoogleDay.parse(security, data)
        i = 0
        for ohlc in iters:
            if i == 0:
                # 2011-04-28,538.06,539.25,534.08,537.97,2037378
                self.assertTrue(isinstance(ohlc.date, date))
                self.assertEqual(ohlc.open, 538.06)
                self.assertEqual(ohlc.high, 539.25)
                self.assertEqual(ohlc.low, 534.08)
                self.assertEqual(ohlc.close, 537.97)
                self.assertEqual(ohlc.volume, 2037378.0)
            i += 1


class GoogleReportFetcherTest(unittest.TestCase):

    def test_init(self):
        f = GoogleReportFetcher()
        self.assertEqual(f._base_url, 'http://www.google.com/finance/info')
        self.assertEqual(f._time_out, 20)
        self.assertEqual(f._request_size, 100)

    def test_init_with_arguments(self):
        f = GoogleReportFetcher(base_url='http://www.google.com.hk/finance/info',
                                time_out=10,
                                request_size=50)
        self.assertEqual(f._base_url, 'http://www.google.com.hk/finance/info')
        self.assertEqual(f._time_out, 10)
        self.assertEqual(f._request_size, 50)
        
    def test_init_with_wrong_arguments(self):
        self.assertRaises(AssertionError,
                          GoogleReportFetcher,
                          request_size=200)
        
    def test_fetch(self):
        f = GoogleReportFetcher(request_size=2)
        s1 = GoogleSecurity(SH(), '000001')
        s2 = GoogleSecurity(SH(), '600028')
        s3 = GoogleSecurity(NASDAQ(), 'GOOG')

        def callback(body):
            qs = GoogleReport.parse(body)
            for quote in qs:
                if quote.security == s1:
                    # something must wrong if SSE Composite Index goes down to 100
                    self.assertTrue(quote.price > 100)
        
        f.fetch(s1, s2, s3,
                callback=callback)

    def test_fetch_nyse(self):
        symbols = "NYSE:MMM,NYSE:SVN,NYSE:NDN,NYSE:AHC,NYSE:AIR,NYSE:AAN,NYSE:ABB,NYSE:ABT,NYSE:ANF,NYSE:ABH,NYSE:ABM,NYSE:ABVT,NYSE:AKR,NYSE:ACN,NYSE:ABD,NYSE:AH,NYSE:ACW,NYSE:ACE,NYSE:ATV,NYSE:ATU,NYSE:AYI,NYSE:ADX,NYSE:AGRO,NYSE:PVD,NYSE:AEA,NYSE:AAP,NYSE:AMD,NYSE:ASX,NYSE:AAV,NYSE:ATE,NYSE:AGC,NYSE:AVK,NYSE:LCM,NYSE:ACM,NYSE:ANW,NYSE:AEB,NYSE:AED,NYSE:AEF,NYSE:AEG,NYSE:AEH,NYSE:AEV,NYSE:AER,NYSE:ARX,NYSE:ARO,NYSE:AET,NYSE:AMG,NYSE:AFL,NYSE:AGCO,NYSE:NCV,NYSE:NCZ,NYSE:NIE,NYSE:NGZ,NYSE:NAI,NYSE:A,NYSE:AGL,NYSE:AEM,NYSE:ADC,NYSE:GRO,NYSE:AGU,NYSE:AL,NYSE:APD,NYSE:AYR,NYSE:ARG,NYSE:AKS,NYSE:ABA/CL,NYSE:ALM,NYSE:ALP^N,NYSE:ALP^O,NYSE:ALP^P,NYSE:ALQ/CL,NYSE:ALZ/CL,NYSE:ALG,NYSE:ALK,NYSE:AIN,NYSE:ALB,NYSE:ALU,NYSE:AA,NYSE:ALR,NYSE:ALR^B,NYSE:ALEX,NYSE:ALX,NYSE:ARE,NYSE:ARE^C,NYSE:Y,NYSE:ATI,NYSE:AGN,NYSE:ALE,NYSE:AKP,NYSE:AB,NYSE:ADS,NYSE:AIQ,NYSE:AFB,NYSE:AYN,NYSE:AOI,NYSE:AWF,NYSE:ACG,NYSE:LNT,NYSE:ATK,NYSE:AFC,NYSE:AIB"
        symbols = symbols.split(',')

        symbols = [GoogleSecurity.from_string(s) for s in symbols]

        f = GoogleReportFetcher()

        def callback(body):
            rs = GoogleReport.parse(body)
            for r in rs:
                pass

        f.fetch(*symbols, callback=callback)


class GoogleDayFetcherTest(unittest.TestCase):

    def test_init(self):
        f = GoogleDayFetcher()
        self.assertEqual(f._base_url, 'http://www.google.com/finance/historical')
        self.assertEqual(f._time_out, 20)
        self.assertEqual(f._max_clients, 10)

    def test_init_with_wrong_arguments(self):
        self.assertRaises(AssertionError,
                          GoogleReportFetcher,
                          max_clients=20)
        
    def test_fetch(self):
        f = GoogleDayFetcher()
        s1 = GoogleSecurity(NASDAQ(), 'GOOG')
        s2 = GoogleSecurity(NASDAQ(), 'AAPL')

        def callback(security, body):
            iters = GoogleDay.parse(security, body)
            i = 0
            for ohlc in iters:
                self.assertTrue(ohlc.security in (s1, s2))
                if i == 0 and ohlc.security == s1:
                    self.assertEqual(str(ohlc.date), "2011-04-28")
                    self.assertEqual(ohlc.open, 538.06)
                    self.assertEqual(ohlc.high, 539.25)
                    self.assertEqual(ohlc.low, 534.08)
                    self.assertEqual(ohlc.close, 537.97)
                    self.assertEqual(ohlc.volume, 2037378.0)
                    
                i += 1

        start_date = datetime.strptime("2011-04-01", "%Y-%m-%d").date()
        end_date = datetime.strptime("2011-04-28", "%Y-%m-%d").date()
        f.fetch(s1, s2,
                callback=callback,
                start_date=start_date,
                end_date=end_date)


if __name__ == '__main__':
    unittest.main()
