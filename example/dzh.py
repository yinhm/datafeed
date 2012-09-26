#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2011 yinhm
import datetime
import io
import os
import sys

import numpy as np

ROOT_PATH = os.path.join(os.path.realpath(os.path.dirname(__file__)), '..')
sys.path[0:0] = [ROOT_PATH]

from cStringIO import StringIO
from datafeed.client import Client
from datafeed.datastore import Manager
from datafeed.exchange import *
from datafeed.providers.dzh import *

def import_min():
        
    var_path = os.path.join(ROOT_PATH, 'var')
    
    client = Client()
    store = Manager('/tmp/df', SH())
    
    filename = os.path.join(var_path, "dzh/sh/MIN1.DAT")
    io = DzhMinute()
    for symbol, ohlcs in io.read(filename, 'SH'):
        client.put_minute(symbol, ohlcs)
    
    filename = os.path.join(var_path, "dzh/sh/MIN1.DAT")
    io = DzhMinute()
    for symbol, ohlcs in io.read(filename, 'SH'):
        for ohlc in ohlcs:
            ohlc['time'] = ohlc['time'] - 8 * 3600
        print symbol
        #client.put_1minute(symbol, ohlcs)
        store.oneminstore.update(symbol, ohlcs)
    
    
    filename = os.path.join(var_path, "dzh/sh/MIN.DAT")
    io = DzhFiveMinute()
    for symbol, ohlcs in io.read(filename, 'SH'):
        for ohlc in ohlcs:
            ohlc['time'] = ohlc['time'] - 8 * 3600
        print symbol
        client.put_5minute(symbol, ohlcs)
        # store.fiveminstore.update(symbol, ohlcs)


def import_fin():
    import time

    io = DzhFin()

    filename = os.path.join(os.path.realpath(os.path.dirname(__file__)), 'full.FIN')
    if os.path.exists(filename):
        data = open(filename).read()
        io.f = StringIO(data)
        io._fetched = True

    for r in io.read():
        print r[0], r[1]


if __name__ == '__main__':
    import_fin()
        
