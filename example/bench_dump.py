#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2011 yinhm
import datetime
import h5py
import os
import shelve
import sys
import timeit

import cPickle as pickle
import numpy as np

ROOT_PATH = os.path.join(os.path.realpath(os.path.dirname(__file__)), '..')
sys.path[0:0] = [ROOT_PATH]

from cStringIO import StringIO
from datafeed.client import Client
from datafeed.datastore import *
from datafeed.exchange import *
from datafeed.providers.dzh import *

var_path = os.path.join(ROOT_PATH, 'var')
store = Manager('/tmp/df', SH())
filename = os.path.join(var_path, "20101202.h5")
date = datetime.datetime.strptime('20101202', '%Y%m%d').date()

hdf_store = h5py.File(filename)
f1 = NumpyFile(hdf_store, date, SH().market_minutes)
f2 = shelve.open('/tmp/dump.shelve')

def f1_bench_read():
    for k, v in hdf_store.iteritems():
        if isinstance(v, h5py.Group):
            continue
        f1[str(k)] = v[:]

def f1_bench_dump():
    pickle.dump(f1, open('/tmp/dump.pickle', 'wb'), -1)


def f2_bench_read():
    for k, v in hdf_store.iteritems():
        if isinstance(v, h5py.Group):
            continue
        f2[str(k)] = v[:]

def f2_bench_dump():
    f2.close()


if __name__ == '__main__':
    d = 1

    timer = timeit.Timer(stmt='f1_bench_read()',
                         setup="from __main__ import f1_bench_read")
    result = timer.timeit(number=d)
    print result

    timer = timeit.Timer(stmt='f1_bench_dump()',
                         setup="from __main__ import f1_bench_dump")
    result = timer.timeit(number=d)
    print result

    timer = timeit.Timer(stmt='f2_bench_read()',
                         setup="from __main__ import f2_bench_read")
    result = timer.timeit(number=d)
    print result

    timer = timeit.Timer(stmt='f2_bench_dump()',
                         setup="from __main__ import f2_bench_dump")
    result = timer.timeit(number=d)
    print result

