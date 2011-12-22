#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2011 yinhm
import datetime
import os
import sys

import numpy as np


ROOT_PATH = os.path.join(os.path.realpath(os.path.dirname(__file__)), '..')
sys.path[0:0] = [ROOT_PATH]

from cStringIO import StringIO
from pandas import DataFrame

from datafeed.client import Client
from  datafeed.dividend import Dividend

client = Client()
symbol = 'SH600036'

y = client.get_day(symbol, 1000)
dividends = client.get_dividend(symbol)

index = np.array([datetime.date.fromtimestamp(v) for v in y['time']],
                 dtype=object)
y = DataFrame.from_records(y, index=index, exclude=['time'])

print dividends

for div in dividends:
    d = Dividend(div)
    d.adjust(y)

day = '20080725'
print datetime.datetime.fromtimestamp(client.get_day(symbol, day)['time'])

d1 = client.get_day(symbol, day)
print d1

