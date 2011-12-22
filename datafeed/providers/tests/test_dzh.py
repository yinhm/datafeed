#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import with_statement

import os
import unittest
from cStringIO import StringIO

from datafeed.providers.dzh import *

class DzhDayTest(unittest.TestCase):

    def assertFloatEqual(self, actual, expt):
        if abs(actual - expt) < 0.1 ** 5:
            return True
        return False

    def test_read_generator(self):
        path = os.path.join(os.path.realpath(os.path.dirname(__file__)),
                            '../../../var')

        filename = os.path.join(path, "dzh/sh/DAY.DAT")
        io = DzhDay()
        f = io.read(filename, 'SH')
        symbol, ohlcs = f.next()

        self.assertEqual(symbol, "SH000001")

        ohlc = ohlcs[0]
        
        self.assertEqual(ohlc['time'], 661564800)
        self.assertFloatEqual(ohlc['open'], 96.05)
        self.assertFloatEqual(ohlc['close'], 99.98)
        self.assertFloatEqual(ohlc['volume'], 1260.0)
        self.assertFloatEqual(ohlc['amount'], 494000.0)


class DzhDividendTest(unittest.TestCase):

    def test_read_generator(self):
        io = DzhDividend()
        r = io.read()
        data = r.next()
        
        self.assertEqual(data[0], "SZ000001")
        
        divs = data[1]
        self.assertEqual(divs[0]['time'], 701308800)
        self.assertEqual(divs[0]['split'], 0.5)
        self.assertTrue(abs(divs[0]['dividend'] - 0.20) < 0.000001)
        


class DzhSectorTest(unittest.TestCase):

    def test_read_generator(self):
        io = DzhSector()
        r = io.read()
        sector, options = r.next()

        self.assertEqual(sector, "行业")
        self.assertTrue(options.has_key("工程建筑"))
        self.assertTrue(len(options["工程建筑"]) > 0)


if __name__ == '__main__':
    unittest.main()
