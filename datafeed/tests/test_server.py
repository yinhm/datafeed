from __future__ import with_statement

import datetime
import gc
import json
import marshal
import re
import os
import shutil
import time
import unittest
import zlib

from datafeed.exchange import SH
from datafeed.server import Application, Handler
from datafeed.server import Request
from datafeed.tests import helper

from mock import Mock, patch


class MockRequest(Request):

    def write(self, chunk):
        self.result = chunk

class HandlerTest(unittest.TestCase):
    def setUp(self):
        self.datadir = '/tmp/datafeed-%d' % int(time.time() * 1000 * 1000)
        os.mkdir(self.datadir)

        self.app = Application(self.datadir, SH(), rdb=True)
        self.app.dbm._mtime = time.time()

    def tearDown(self):
        shutil.rmtree(self.datadir, ignore_errors=True)

    def test_get_put_tick(self):
        symbol = helper.sample_key()
        sample = helper.sample()
        timestamp = time.time()
        sample[symbol]['timestamp'] = timestamp
        expected = sample[symbol]

        data = zlib.compress(json.dumps(expected))
        request = MockRequest(None, 'put_tick', symbol, timestamp, data, 'zip')
        self.app(request)
        self.assertEqual('+OK\r\n', request.result)

        request = MockRequest(None, 'get_tick', symbol, 'json')
        self.app(request)

        data = request.result.split('\r\n')[1]
        result = json.loads(data)
        self.assertTrue(expected, result)

        expected = result
        iter = self.app.dbm.tick.query(timestamp)
        keys = list(iter)
        key = keys[0]
        rawdata = self.app.dbm.tick.get(key)
        actual = json.loads(rawdata)
        self.assertTrue(expected, actual)


    def test_get_put_depth(self):
        symbol = helper.sample_key()
        timestamp = time.time()
        depth = {"bids": [[2767, 16.3121], ["2766.5", 0.004], ["2766.04", 0.01], [2766, 0.004], ["2765.85", 1], ["2765.5", 0.002], ["2765.44", 0.362], ["2765.04", 0.01], [2765, 10.003], ["2764.5", 0.001]], "asks": [["2871.21", 0.01], ["2870.62", 0.2652], [2870, 17.8974], [2869, 5.3053], ["2868.09", 0.01], [2868, 11.1509], ["2867.09", 0.01], [2867, 234.221], ["2866.09", 0.01], ["2866.03", 1]]}

        data = zlib.compress(json.dumps(depth))
        request = MockRequest(None, 'put_depth', symbol, timestamp, data, 'zip')
        self.app(request)
        self.assertEqual('+OK\r\n', request.result)

        request = MockRequest(None, 'get_depth', symbol, 'json')
        self.app(request)

        data = request.result.split('\r\n')[1]
        result = json.loads(data)
        self.assertTrue(depth, result)

        actual = self.app.dbm.depth.get('cached_depth_SH000001')
        self.assertTrue(depth, actual)


    def test_get_put_trade(self):
        symbol = helper.sample_key()
        timestamp = time.time()
        trade = {"date":1378035025,"price":806.37,"amount":0.46,"tid":1,"type":"sell"}

        data = zlib.compress(json.dumps(trade))
        request = MockRequest(None, 'put_trade', symbol, timestamp, data, 'zip')
        self.app(request)
        self.assertEqual('+OK\r\n', request.result)

        request = MockRequest(None, 'get_trade', symbol, 'json')
        self.app(request)

        data = request.result.split('\r\n')[1]
        result = json.loads(data)
        self.assertTrue(trade, result)

        actual = self.app.dbm.trade.get('cached_trade_SH000001')
        self.assertTrue(trade, actual)


if __name__ == '__main__':
    unittest.main()
