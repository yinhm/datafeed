#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2010 yinhm

'''A datafeed server daemon.
'''

import datetime
import logging
import marshal
import os
import sys
import time
import tornado

from tornado import ioloop
from tornado.options import define, options


sys.path[0:0] = ['..']


from datafeed.exchange import SH
from datafeed.imiguserver import ImiguApplication
from datafeed.server import Server, Request


tornado.options.parse_command_line()
app = ImiguApplication('/tmp/df', SH())


today = datetime.datetime.today()
timestamp = int(time.mktime((today.year, today.month, today.day,
                             15, 0, 0, 0, 0, 0)))
dt = datetime.datetime.fromtimestamp(timestamp)
        
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

app.dbm.update_ticks(d)

path = os.path.dirname(os.path.realpath(__file__))
f = open(path + '/../datafeed/tests/ticks.dump', 'r')
data = marshal.load(f)
for v in data.itervalues():
    if 'amount' not in v:
        continue
    v['time'] = str(dt)
    v['timestamp'] = timestamp
app.dbm.update_ticks(data)

request = Request(None, 'archive_minute')
app(request)
    

def main():
    request = Request(None, 'archive_minute')
    app(request)

if __name__ == "__main__":
    import cProfile
    cProfile.run('main()', '/tmp/fooprof')
