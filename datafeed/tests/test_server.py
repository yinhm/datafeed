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

    def test_put_tick(self):
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


if __name__ == '__main__':
    unittest.main()
