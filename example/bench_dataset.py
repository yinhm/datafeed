#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2011 yinhm
import datetime
import h5py
import os
import random
import sys
import time
import timeit

import numpy as np

DTYPE = np.dtype({'names': ('time', 'price', 'volume', 'amount'),
                  'formats': ('i4', 'f4', 'f4', 'f4')})


def bench_ds():
    filename = '/tmp/bench-%d.h5' % int(time.time())

    symbols = ["SH%.6d" % i for i in xrange(10000)]

    f = h5py.File(filename)
    for symbol in symbols:
        f.create_dataset(symbol, (240, ), DTYPE)
    f.close()
    
    for x in xrange(10):
        # open for bench again
        f = h5py.File(filename)
        random.shuffle(symbols)
        for symbol in symbols:
            ds = f[symbol]
        f.close()
    

def require_dataset(handle, symbol):
    gid = symbol[:3]
    group = handle.require_group(gid)
    try:
        ds = group[symbol]
    except KeyError:
        ds = group.create_dataset(symbol, (240, ), DTYPE)
    return ds

def dataset(handle, symbol):
    path = "%s/%s" % (symbol[:3], symbol)
    return handle[path]


def bench_grouped_ds():
    filename = '/tmp/bench-%d.h5' % int(time.time())

    symbols = ["SH%.6d" % i for i in xrange(10000)]

    f = h5py.File(filename)
    for symbol in symbols:
        require_dataset(f, symbol)
    f.close()

    for x in xrange(10):
        # open for bench again
        f = h5py.File(filename)
        random.shuffle(symbols)
        for symbol in symbols:
            ds = dataset(f, symbol)
        f.close()


if __name__ == '__main__':
    d = 1
    
    ds_timer = timeit.Timer(stmt='bench_ds()',
                            setup="from __main__ import bench_ds")
    ds_result = ds_timer.timeit(number=d)
    print ds_result

    grouped_ds_timer = timeit.Timer(stmt='bench_grouped_ds()',
                                    setup="from __main__ import bench_grouped_ds")
    grouped_ds_result = grouped_ds_timer.timeit(number=d)
    print grouped_ds_result
