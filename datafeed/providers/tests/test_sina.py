#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import with_statement

import os
import unittest

from datetime import datetime, date
from datafeed.exchange import *
from datafeed.providers.sina import *


class SinaSecurityTest(unittest.TestCase):

    def test_abbr_sh(self):
        s = SinaSecurity(SH(), '600028')
        self.assertEqual(s._abbr, 'sh')

    def test_abbr_sz(self):
        s = SinaSecurity(SZ(), '000001')
        self.assertEqual(s._abbr, 'sz')

    def test_sina_id(self):
        s = SinaSecurity(SH(), '600028')
        self.assertEqual(str(s), 'sh600028')

    def test_abbr_to_exchange(self):
        ex = SinaSecurity.get_exchange_from_abbr("sh")
        self.assertEqual(ex, SH())

    def test_ss_abbr(self):
        ret = SinaSecurity.from_string('sh600028')
        self.assertEqual(ret.exchange, SH())
        self.assertEqual(ret.symbol, '600028')
    

class SinaTickTest(unittest.TestCase):
    _RAW_DATA = '''var hq_str_sh000001="上证指数,2911.510,2911.511,2932.188,2933.460,2890.225,0,0,96402722,102708976572,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2011-05-03,15:03:11";
var hq_str_sh600028="中国石化,8.64,8.64,8.68,8.71,8.58,8.68,8.69,27761321,240634267,11289,8.68,759700,8.67,556338,8.66,455296,8.65,56600,8.64,143671,8.69,341859,8.70,361255,8.71,314051,8.72,342155,8.73,2011-05-03,15:03:11";'''

    def test_sina_tick(self):
        ret = SinaTick.parse(self._RAW_DATA)
        i = 0
        for r in ret:
            if i == 1:
                self.assertEqual(r.security.exchange, SH())
                self.assertEqual(r.security.symbol, '600028')
                self.assertEqual(r.name, '中国石化')
                self.assertEqual(r.open, 8.64)
                self.assertEqual(r.preclose, 8.64)
                self.assertEqual(str(r.date), "2011-05-03")

            i += 1


class SinaTickFetcherTest(unittest.TestCase):

    def test_init(self):
        f = SinaTickFetcher()
        self.assertEqual(f._base_url, 'http://hq.sinajs.cn')
        self.assertEqual(f._time_out, 20)
        self.assertEqual(f._request_size, 100)

    def test_init_with_wrong_arguments(self):
        self.assertRaises(AssertionError,
                          SinaTickFetcher,
                          request_size=200)
        
    def test_fetch(self):
        f = SinaTickFetcher(request_size=2)
        s1 = SinaSecurity(SH(), '000001')
        s2 = SinaSecurity(SH(), '600028')
        s3 = SinaSecurity(SZ(), '000976')

        def callback(body):
            qs = SinaTick.parse(body)
            for quote in qs:
                if quote.security == s1:
                    # something must wrong if SSE Composite Index goes down to 100
                    self.assertTrue(quote.price > 100)
        
        f.fetch(s1, s2, s3,
                callback=callback)


if __name__ == '__main__':
    unittest.main()
