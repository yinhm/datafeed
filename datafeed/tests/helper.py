import datetime
import numpy
import os
import time

datadir = '/tmp/datafeed-%d' % int(time.time() * 1000)
os.mkdir(datadir)

def sample_key():
    return 'SH000001'

def sample():
    dt = datetime.datetime.now()
    timestamp = int(time.time())

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
    return d

def sample_minutes():
    path = os.path.dirname(os.path.realpath(__file__))
    data = numpy.load(os.path.join(path, 'minute.npy'))

    today = datetime.datetime.today()
    for row in data:
        day = datetime.datetime.fromtimestamp(int(row['time']))
        t = time.mktime((today.year, today.month, today.day,
                         day.hour, day.minute, 0, 0, 0, 0))
        row['time'] = int(t)

    return data
