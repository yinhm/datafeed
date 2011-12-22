'''
@FIXME
======
due to client not async we need to start a real server in terminal to perform
this tests.
'''

from __future__ import with_statement

import marshal
import os
import sys
import time
import numpy
import socket
import unittest

import numpy as np

from cStringIO import StringIO

from datetime import datetime
from datafeed.client import Client

class ClientTest(unittest.TestCase):

    def setUp(self):
        self.client = Client()

        today = datetime.today()
        timestamp = int(time.mktime((today.year, today.month, today.day,
                                      10, 30, 0, 0, 0, 0)))
        dt = datetime.fromtimestamp(timestamp)
        
        d = {
            'SH000001' : {
                'amount': 84596203520.0,
                'close': 2856.9899999999998,
                'high': 2880.5599999999999,
                'low': 2851.9499999999998,
                'name': u'\u4e0a\u8bc1\u6307\u6570',
                'open': 2868.73,
                'preclose': 2875.8600000000001,
                'price': 2856.9899999999998,
                'symbol': u'SH000001',
                'time': str(dt),
                'timestamp': timestamp,
                'volume': 75147848.0
                }
            }
        self.client.put_reports(d)

    def test_connect(self):
        self.client.connect()
        self.assertTrue(isinstance(self.client._sock, socket._socketobject))

    def test_put_reports(self):
        path = os.path.dirname(os.path.realpath(__file__))
        r = self.client.get_report('SH000001')
        f = open(os.path.join(path, 'reports.dump'), 'r')
        data = marshal.load(f)
        for v in data.itervalues():
            if 'amount' not in v:
                continue
            v['time'] = r['time']
            v['timestamp'] = r['timestamp']

        ret = self.client.put_reports(data)
        self.assertEqual(ret, 'OK')

    def test_put_empty_reports(self):
        ret = self.client.put_reports({})
        self.assertEqual(ret, 'OK')

    def test_get_list(self):
        stocks = self.client.get_list()
        self.assertTrue(isinstance(stocks, dict))
        self.assertTrue('SH000001' in stocks)

    def test_get_report(self):
        quote = self.client.get_report('SH000001')
        self.assertTrue(isinstance(quote, dict))
        self.assertTrue(isinstance(quote['price'], float))

    def test_get_reports(self):
        stocks = self.client.get_reports('SH000001', 'KeyError')
        self.assertTrue(isinstance(stocks, dict))
        self.assertTrue('SH000001' in stocks)
        self.assertFalse('KeyError' in stocks)

    def test_put_then_get_minute(self):
        path = os.path.dirname(os.path.realpath(__file__))
        data = numpy.load(os.path.join(path, 'minute.npy'))

        symbol = 'SH999999'

        today = datetime.today()
        for row in data:
            day = datetime.fromtimestamp(int(row['time']))
            t = time.mktime((today.year, today.month, today.day,
                             day.hour, day.minute, 0, 0, 0, 0))
            
            row['time'] = int(t)

        self.client.put_minute(symbol, data)

        ret = self.client.get_minute(symbol, int(time.time()))
        self.assertEqual(data['price'].tolist(), ret['price'].tolist())


if __name__ == '__main__':
    unittest.main()
