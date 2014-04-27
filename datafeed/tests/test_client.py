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
        self.client.put_ticks(d)

    def test_connect(self):
        self.client.connect()
        self.assertTrue(isinstance(self.client._sock, socket._socketobject))

    def test_put_ticks(self):
        path = os.path.dirname(os.path.realpath(__file__))
        r = self.client.get_tick('SH000001')
        f = open(os.path.join(path, 'ticks.dump'), 'r')
        data = marshal.load(f)
        for v in data.itervalues():
            v['time'] = r['time']
            v['timestamp'] = r['timestamp']

        ret = self.client.put_ticks(data)
        self.assertEqual(ret, 'OK')

    def test_put_empty_ticks(self):
        ret = self.client.put_ticks({})
        self.assertEqual(ret, 'OK')

    def test_get_list(self):
        stocks = self.client.get_list()
        self.assertTrue(isinstance(stocks, dict))
        self.assertTrue('SH000001' in stocks)

    def test_get_tick(self):
        quote = self.client.get_tick('SH000001')
        self.assertTrue(isinstance(quote, dict))
        self.assertTrue(isinstance(quote['price'], float))

    def test_get_ticks(self):
        stocks = self.client.get_ticks('SH000001', 'KeyError')
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

    def test_get_put_depth(self):
        symbol = "SH01"
        timestamp = time.time()
        depth = {"bids": [[2767, 16.3121], ["2766.5", 0.004], ["2766.04", 0.01], [2766, 0.004], ["2765.85", 1], ["2765.5", 0.002], ["2765.44", 0.362], ["2765.04", 0.01], [2765, 10.003], ["2764.5", 0.001]], "asks": [["2871.21", 0.01], ["2870.62", 0.2652], [2870, 17.8974], [2869, 5.3053], ["2868.09", 0.01], [2868, 11.1509], ["2867.09", 0.01], [2867, 234.221], ["2866.09", 0.01], ["2866.03", 1]]}

        ret = self.client.put_depth(symbol, timestamp, depth)
        self.assertEqual(ret, 'OK')


    def test_get_put_raw_depth(self):
        symbol = "SH01"
        timestamp = time.time()
        depth = '{"bids": [[2767, 16.3121], ["2766.5", 0.004], ["2766.04", 0.01], [2766, 0.004], ["2765.85", 1], ["2765.5", 0.002], ["2765.44", 0.362], ["2765.04", 0.01], [2765, 10.003], ["2764.5", 0.001]], "asks": [["2871.21", 0.01], ["2870.62", 0.2652], [2870, 17.8974], [2869, 5.3053], ["2868.09", 0.01], [2868, 11.1509], ["2867.09", 0.01], [2867, 234.221], ["2866.09", 0.01], ["2866.03", 1]]}'

        ret = self.client.put_depth(symbol, timestamp, depth, jsondata=True)
        self.assertEqual(ret, 'OK')


    def test_get_put_trade(self):
        symbol = "SH01"
        timestamp = time.time()
        trade = {"date":1378035025,"price":806.37,"amount":0.46,"tid":1,"type":"sell"}

        ret = self.client.put_trade(symbol, timestamp, trade)
        self.assertEqual(ret, 'OK')


    def test_get_put_trade_meta(self):
        key = "last_trade_log"
        ret = self.client.put_meta(key, 'xx')
        self.assertEqual(ret, 'OK')

        ret = self.client.get_meta(key)
        self.assertEqual('xx', ret)


if __name__ == '__main__':
    unittest.main()
