from __future__ import with_statement

import binascii
import h5py
import json
import logging
import os
import gc
import re
import shutil
import time
import unittest
import numpy as np

from datetime import datetime

from mock import Mock, patch

from datafeed import datastore
from datafeed import transform
from datafeed.exchange import SH
from datafeed.datastore import *
from datafeed.tests import helper

class TestHelper(object):

    def _setup(self):
        self.datadir = '/tmp/datafeed-%d' % int(time.time() * 1000 * 1000)
        os.mkdir(self.datadir)

    def _clean(self):
        shutil.rmtree(self.datadir, ignore_errors=True)


class ManagerTest(unittest.TestCase, TestHelper):

    def setUp(self):
        self._setup()
        self.manager = Manager(self.datadir, SH())

    def tearDown(self):
        self.manager.clean()
        self._clean()

    def test_store_filename(self):
        ret = self.manager._store
        self.assertEqual(ret.filename, '%s/data.h5' % self.datadir)
        self.assertTrue(isinstance(ret, h5py.File))

    def test_daystore(self):
        ret = self.manager.daystore
        self.assertTrue(isinstance(ret, Day))

    def test_not_inited_minutestore(self):
        ret = self.manager._minutestore
        self.assertEqual(ret, None)

    def test_init_manager_with_minute_store(self):
        self.manager.set_mtime(1291341180)
        self.assertTrue(isinstance(self.manager.minutestore, Minute))
        self.assertTrue(isinstance(self.manager.minutestore.handle, MinuteSnapshotCache))

    def test_minute_filename_market_not_open(self):
        # not open yet
        ts = 1291312380
        self.manager.set_mtime(ts)
        date = datetime.fromtimestamp(ts).date()
        self.assertEqual(date, self.manager.minutestore.date)
        self.assertEqual('/minsnap/20101203', self.manager.minutestore.pathname)
    
    def test_minute_filename_opened(self):
        # in session
        ts = 1291341180
        date = datetime.fromtimestamp(ts).date()
        self.manager.set_mtime(ts)
        self.assertEqual(date, self.manager.minutestore.date)
        self.assertEqual('/minsnap/20101203', self.manager.minutestore.pathname)
 
    def test_rotate_minute_store(self):
        dbm = self.manager
        dbm.set_mtime(1291341180)
        self.assertTrue(isinstance(dbm.minutestore.handle, MinuteSnapshotCache))

        dbm.set_mtime(1291341180 + 86400)
        dbm.rotate_minute_store()
        self.assertEqual('/minsnap/20101204', dbm.minutestore.pathname)

    def test_get_minutestore(self):
        store = self.manager.get_minutestore_at(1291341180)
        self.assertTrue(isinstance(store, Minute))
        self.assertEqual('/minsnap/20101203', store.pathname)

    def test_update_day_should_call_to_correctly_store(self):
        p1 = {'time': int(time.time())}
        data = [p1]
        store = Mock()

        self.manager.get_minutestore_at = Mock(return_value=store)
        self.manager.update_minute("SH000001", data)
        self.manager.get_minutestore_at.assert_called_with(p1['time'])
    
    def test_get_minutestore_force_cache(self):
        store = self.manager.get_minutestore_at(1291341180, memory=True)
        self.assertTrue(isinstance(store.handle, MinuteSnapshotCache))

    def test_get_minutestore_force_no_cache(self):
        ts = int(time.time())
        store = self.manager.get_minutestore_at(ts, memory=False)
        self.assertTrue(isinstance(store.handle, h5py.Group))

    def test_get_minutestore_default_cache(self):
        ts = int(time.time())
        store = self.manager.get_minutestore_at(ts)
        self.assertTrue(isinstance(store.handle, MinuteSnapshotCache))

    def test_5minstore(self):
        ret = self.manager.fiveminstore
        self.assertTrue(isinstance(ret, FiveMinute))

    def test_tickstore(self):
        ret = self.manager.tickstore
        self.assertTrue(isinstance(ret, datastore.Tick))

    def test_sectorstore(self):
        ret = self.manager.sectorstore
        self.assertTrue(isinstance(ret, datastore.Sector))

    def test_dividendstore(self):
        ret = self.manager.divstore
        self.assertTrue(isinstance(ret, datastore.Dividend))

    def test_tick_history(self):
        ret = self.manager.tick
        self.assertTrue(isinstance(ret, datastore.TickHistory))

    def test_depth_history(self):
        ret = self.manager.depth
        self.assertTrue(isinstance(ret, datastore.DepthHistory))

    def test_trade_history(self):
        ret = self.manager.trade
        self.assertTrue(isinstance(ret, datastore.TradeHistory))

    def test_disable_rdb(self):
        self._clean()
        self._setup()
        dbm = Manager(self.datadir, SH(), enable_rdb=False)
        self.assertEqual(dbm._rstore, None)

class DictStoreTest(unittest.TestCase):

    def test_init_store(self):
        filename = '%s/dstore_init.dump' % helper.datadir
        data = {'r1': 'v1'}
        ds = DictStore(filename, data)
        r1 = ds['r1']
        self.assertTrue(r1, 'v1')

    def test_reopen_file(self):
        filename = '%s/dstore_reopen.dump' % helper.datadir

        data = {'r1': 'v1'}
        ds = DictStore(filename, data)
        ds.close()

        ds = DictStore.open(filename)
        r1 = ds['r1']
        self.assertTrue(r1, 'v1')


class DictStoreNamespaceTest(unittest.TestCase):

    def setUp(self):
        class Impl(DictStoreNamespace):
            pass
        filename = '%s/dsn_impl.dump' % helper.datadir
        self.store = DictStore(filename, {})
        self.impl = Impl(self.store)

    def test_inited_impl(self):
        self.assertTrue(self.store.has_key('impl'))
        self.assertEqual(self.impl.keys(), [])

    def test_set_and_get_item(self):
        self.impl['k12'] = 'v21'
        self.assertEqual(self.impl['k12'], 'v21')

    def test_set_and_get_item2(self):
        self.impl['k12'] = 'v21'
        self.assertEqual(self.impl.get('k12'), 'v21')


class TickTest(unittest.TestCase, TestHelper):

    def setUp(self):
        self._setup()

    def tearDown(self):
        self._clean()

    def test_init_store(self):
        filename = '%s/dstore.dump' % self.datadir
        store = DictStore.open(filename)
        rstore = Tick(store)
        sample = helper.sample()

        rstore.update(sample)
        key = 'SH000001'
        self.assertEqual(rstore[key], sample[key])

        store.close()
        self.assertRaises(AssertionError, rstore.set, key, sample)
        self.assertRaises(AssertionError, rstore.get, key)

        store = DictStore.open(filename)
        rstore = Tick(store)
        self.assertEqual(rstore[key], sample[key])


class TickHistoryTest(unittest.TestCase, TestHelper):

    def setUp(self):
        self._setup()

        self.store = datastore.RockStore.open(self.datadir)
        self.tick = datastore.TickHistory(self.store)

    def tearDown(self):
        del(self.tick)
        del(self.store)
        gc.collect()
        self._clean()

    def test_get_none(self):
        self.assertIsNone(self.tick._get('xxx'))

    def test_put_get(self):
        self.tick._put(b"a", b"b")
        self.assertEqual(b"b", self.tick._get(b"a"))

    def test_init_store(self):
        ts = int(time.time())
        key = self.tick.put('btc', ts, b"v")
        self.assertEqual(b"v", self.store.get(key))

    def test_put_with_prefix_key(self):
        ts = 1397805240
        key = self.tick.put('btc', ts, b"v2")
        self.assertTrue(key.startswith('\x00\x014'))

    def test_get_with_bytes_key(self):
        ts = 1397805240
        hexstr = '0001349728103d28500a5348303030303031'
        key = binascii.unhexlify(hexstr)
        self.assertIsNone(self.tick.get(key))

    def test_prefix_key(self):
        self.assertTrue(self.tick.prefix(1397805240), '\x00\x014')

    def test_ticks(self):
        data = """{"BTC": {"sell": "3079.86", "buy": "3078.83", "last": "3079.86", "vol": 88219.1364, "timestamp": 1397805240, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3079.86", "buy": "3078.83", "last": "3079.86", "vol": 88219.1364, "timestamp": 1397805245, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3079.78", "buy": "3075.04", "last": "3079.8", "vol": 88219.3364, "timestamp": 1397805250, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3076", "buy": "3075.21", "last": "3079.8", "vol": 88219.3364, "timestamp": 1397805270, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3075.98", "buy": "3075.22", "last": "3075.98", "vol": 88219.7918, "timestamp": 1397805275, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3075.99", "buy": "3075.23", "last": "3076", "vol": 88225.9708, "timestamp": 1397805280, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3077.8", "buy": "3076.08", "last": "3077.8", "vol": 88226.2321, "timestamp": 1397805285, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3077.8", "buy": "3076.09", "last": "3077.8", "vol": 88226.4575, "timestamp": 1397805290, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3077.78", "buy": "3076", "last": "3076", "vol": 88232.1352, "timestamp": 1397805295, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3077.78", "buy": "3076", "last": "3077.78", "vol": 88232.3352, "timestamp": 1397805305, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3076.9", "buy": "3075.18", "last": "3075.18", "vol": 88233.1607, "timestamp": 1397805310, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3076.9", "buy": "3075.18", "last": "3076.9", "vol": 88233.1862, "timestamp": 1397805315, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3077.16", "buy": "3076", "last": "3077.18", "vol": 88233.5862, "timestamp": 1397805320, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3076.89", "buy": "3075", "last": "3076.9", "vol": 88237.9071, "timestamp": 1397805325, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3076.87", "buy": "3075", "last": "3076.89", "vol": 88238.1071, "timestamp": 1397805330, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3076.83", "buy": "3075.01", "last": "3076.89", "vol": 88238.1071, "timestamp": 1397805335, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3076.83", "buy": "3075.01", "last": "3076.85", "vol": 88238.3071, "timestamp": 1397805340, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3076.79", "buy": "3075.01", "last": "3076.79", "vol": 88238.5325, "timestamp": 1397805350, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3076.7", "buy": "3075.01", "last": "3076.7", "vol": 88238.758, "timestamp": 1397805355, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3076.79", "buy": "3075.01", "last": "3076.79", "vol": 88239.9234, "timestamp": 1397805360, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3076.79", "buy": "3075.1", "last": "3076.79", "vol": 88239.9489, "timestamp": 1397805370, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3076.79", "buy": "3075.13", "last": "3076.79", "vol": 88239.9489, "timestamp": 1397805380, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3076.79", "buy": "3075.13", "last": "3076.79", "vol": 88239.9489, "timestamp": 1397805385, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3076.79", "buy": "3075.1", "last": "3075.1", "vol": 88243.9744, "timestamp": 1397805390, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3076.79", "buy": "3075.1", "last": "3075.1", "vol": 88243.9744, "timestamp": 1397805395, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3076.79", "buy": "3075.2", "last": "3076.79", "vol": 88243.9999, "timestamp": 1397805400, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3077.16", "buy": "3074.1", "last": "3074.1", "vol": 88274.5202, "timestamp": 1397805405, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3076.21", "buy": "3075.12", "last": "3076.21", "vol": 88274.5457, "timestamp": 1397805410, "high": "3135", "low": "3038"}}
{"BTC": {"sell": "3076.21", "buy": "3075.18", "last": "3076.21", "vol": 88274.5912, "timestamp": 1397805425, "high": "3135", "low": "3038"}}"""

        keys = []
        for tick in data.split('\n'):
            t = json.loads(tick)
            key = self.tick.put('btc', t['BTC']['timestamp'], tick)
            keys.append(key)

        iter = self.tick.query(1397805240)
        results = list(iter)
        self.assertTrue(keys[0] in results)
        self.assertTrue(keys[-1] in results)
        self.assertEqual(len(results), len(keys))

    def test_batch_write(self):
        trades = [{"date":1378035025,"price":806.37,"amount":0.46,"tid":1,"type":"sell"},
                  {"date":1378035025,"price":810,"amount":0.56,"tid":2,"type":"buy"},
                  {"date":1378035025,"price":806.37,"amount":4.44,"tid":3,"type":"sell"},
                  {"date":1378035025,"price":803.2,"amount":0.8,"tid":4,"type":"buy"},
                  {"date":1378035045,"price":804.6,"amount":1.328,"tid":5,"type":"buy"}]
        self.tick.mput('SH01', trades)
        iter = self.tick.query_values(1378035025)
        results = [json.loads(row) for row in list(iter)]
        self.assertEqual(len(trades), len(results))
        self.assertTrue(trades[0] in results)
        self.assertTrue(trades[-1] in results)


class MetaTest(unittest.TestCase, TestHelper):

    def setUp(self):
        self._setup()

        self.store = datastore.RockStore.open(self.datadir)
        self.meta = datastore.Meta(self.store)

    def tearDown(self):
        del(self.meta)
        del(self.store)
        gc.collect()
        self._clean()

    def test_is_none(self):
        ret = self.meta.get("a1")
        self.assertIsNone(None, ret)

    def test_get_put(self):
        self.meta.put("a", time.time(), "b")
        ret = self.meta.get("a")
        self.assertEqual(b"b", ret)

class DayTest(unittest.TestCase):

    def setUp(self):
        self.store = Day(h5py.File('%s/data.h5' % helper.datadir))

    def test_namespace(self):
        h = self.store.handle
        self.assertTrue(isinstance(h, h5py.Group))
        self.assertEqual(h.name, '/day')

    def test_get_from_not_exist_symbol(self):
        key = 'SH987654'
        self.assertRaises(KeyError, self.store.get, symbol=key, length=1)


class MinuteTest(unittest.TestCase):

    def setUp(self):
        ts = int(time.mktime((2011, 1, 1, 1, 1, 0, 0, 0, 0)))
        date = datetime.fromtimestamp(ts).date()
        self.store = Minute(h5py.File('%s/data.h5' % helper.datadir),
                            date,
                            SH().market_minutes)

    def test_namespace(self):
        h = self.store.handle
        self.assertTrue(isinstance(h, h5py.Group))
        self.assertEqual(h.name, '/minsnap/20110101')

    def test_get_from_not_exist_symbol(self):
        key = 'SH987654'
        self.assertRaises(KeyError, self.store.get, symbol=key)


class OneMinuteTest(unittest.TestCase):

    def setUp(self):
        self.store = OneMinute(h5py.File('%s/data.h5' % helper.datadir))

    def test_namespace(self):
        h = self.store.handle
        self.assertTrue(isinstance(h, h5py.Group))
        self.assertEqual(h.name, '/1min')

    def test_get_from_not_exist_symbol(self):
        key = 'SH987654'
        self.assertRaises(KeyError, self.store.get, symbol=key, date=datetime.today())

    def test_get_after_update(self):
        key = 'SH000001'
        date = datetime.fromtimestamp(1316588100)
        x = np.array([
                (1316588100, 3210.860107421875, 3215.239990234375, 3208.43994140625,
                 3212.919921875, 62756.0, 49122656.0),
                (1316588400, 3213.43994140625, 3214.47998046875, 3206.800048828125,
                 3206.840087890625, 81252.0, 55866096.0)
                ], dtype=FiveMinute.DTYPE)
        self.store.update(key, x)

        y = self.store.get(key, date)
        np.testing.assert_array_equal(y, x)

    def test_update_multi_days(self):
        key = 'SH000001'
        x = np.array([
                (1316501700, 3130.8701171875, 3137.739990234375, 3128.81005859375,
                 3132.580078125, 30530.0, 20179424.0),
                (1316502000, 3132.68994140625, 3142.75, 3129.8798828125,
                 3141.5400390625, 57703.0, 41456768.0),
                (1316588100, 3210.860107421875, 3215.239990234375, 3208.43994140625,
                 3212.919921875, 62756.0, 49122656.0),
                (1316588400, 3213.43994140625, 3214.47998046875, 3206.800048828125,
                 3206.840087890625, 81252.0, 55866096.0)
                ], dtype=FiveMinute.DTYPE)
        self.store.update(key, x)

        date = datetime.fromtimestamp(1316501700).date()
        y = self.store.get(key, date)
        np.testing.assert_array_equal(y, x[:2])

        date = datetime.fromtimestamp(1316588400).date()
        y = self.store.get(key, date)
        np.testing.assert_array_equal(y, x[2:])

    def test_update_partial_data(self):
        market_minutes = 60 * 24 # assume 1min data
        store = OneMinute(h5py.File('%s/data.h5' % helper.datadir),
                          market_minutes)
        self.assertEqual(store.time_interval, 60)
        self.assertEqual(store.shape_x, 1440)

        key = '999'
        path = os.path.dirname(os.path.realpath(__file__))
        data = np.load(os.path.join(path, '001.npy'))

        store.update(key, data)

        date = datetime.fromtimestamp(1397621820).date()
        y = store.get(key, date)
        row1, row2 = y[737], y[1036]
        np.testing.assert_array_equal(row1, data[0])
        np.testing.assert_array_equal(row2, data[-1])


class FiveMinuteTest(unittest.TestCase):

    def setUp(self):
        self.store = FiveMinute(h5py.File('%s/data.h5' % helper.datadir))

    def test_namespace(self):
        h = self.store.handle
        self.assertTrue(isinstance(h, h5py.Group))
        self.assertEqual(h.name, '/5min')

    def test_get_from_not_exist_symbol(self):
        key = 'SH987654'
        self.assertRaises(KeyError, self.store.get, symbol=key, date=datetime.today())

    def test_get_after_update(self):
        key = 'SH000001'
        date = datetime.fromtimestamp(1316588100)
        x = np.array([
                (1316588100, 3210.860107421875, 3215.239990234375, 3208.43994140625,
                 3212.919921875, 62756.0, 49122656.0),
                (1316588400, 3213.43994140625, 3214.47998046875, 3206.800048828125,
                 3206.840087890625, 81252.0, 55866096.0)
                ], dtype=FiveMinute.DTYPE)
        self.store.update(key, x)

        y = self.store.get(key, date)
        np.testing.assert_array_equal(y, x)

    def test_update_multi_days(self):
        key = 'SH000001'
        x = np.array([
                (1316501700, 3130.8701171875, 3137.739990234375, 3128.81005859375,
                 3132.580078125, 30530.0, 20179424.0),
                (1316502000, 3132.68994140625, 3142.75, 3129.8798828125,
                 3141.5400390625, 57703.0, 41456768.0),
                (1316588100, 3210.860107421875, 3215.239990234375, 3208.43994140625,
                 3212.919921875, 62756.0, 49122656.0),
                (1316588400, 3213.43994140625, 3214.47998046875, 3206.800048828125,
                 3206.840087890625, 81252.0, 55866096.0)
                ], dtype=FiveMinute.DTYPE)
        self.store.update(key, x)

        date = datetime.fromtimestamp(1316501700).date()
        y = self.store.get(key, date)
        np.testing.assert_array_equal(y, x[:2])

        date = datetime.fromtimestamp(1316588400).date()
        y = self.store.get(key, date)
        np.testing.assert_array_equal(y, x[2:])

    def test_update_multi_partial_days_data(self):
        market_minutes = 1440 # 5min data
        store = FiveMinute(h5py.File('%s/data.h5' % helper.datadir),
                           market_minutes)
        self.assertEqual(store.time_interval, 300)
        self.assertEqual(store.shape_x, 288)

        key = '9991'
        path = os.path.dirname(os.path.realpath(__file__))
        data = np.load(os.path.join(path, '005.npy'))

        store.update(key, data)

        date = datetime.fromtimestamp(data[0]['time']).date()
        y1 = store.get(key, date)
        np.testing.assert_array_equal(y1[196], data[0])

        date = datetime.fromtimestamp(data[-1]['time']).date()
        y2 = store.get(key, date)
        np.testing.assert_array_equal(y2[206], data[-1])

    def test_update_multi_hold_data(self):
        market_minutes = 1440 # 5min data
        store = FiveMinute(h5py.File('%s/data.h5' % helper.datadir),
                           market_minutes)
        key = '9992'
        path = os.path.dirname(os.path.realpath(__file__))
        data = np.load(os.path.join(path, '005_na.npy'))

        store.update(key, data)

        date = datetime.fromtimestamp(data[-1]['time']).date()
        y2 = store.get(key, date)

        # Data has holes between index 171 and index 172.
        np.testing.assert_array_equal(y2[0], data[132])
        np.testing.assert_array_equal(y2[167], data[-1])
        np.testing.assert_array_equal(y2[39], data[171])
        np.testing.assert_array_equal(y2[43], data[172])


class MinuteSnapshotCacheTest(unittest.TestCase, TestHelper):

    def setUp(self):
        self._setup()

        self.filename = '%s/dstore_mincache.dump' % self.datadir
        self.date = datetime.today().date()
        self.store = DictStore.open(self.filename)
        self.mstore = MinuteSnapshotCache(self.store, self.date)

    def tearDown(self):
        self._clean()

    def test_inited_date(self):
        self.assertEqual(self.mstore.date, datetime.today().date())

    def test_true_of_store(self):
        ms = Minute(self.mstore, datetime.today().date(), SH().market_minutes)
        self.assertTrue(ms)

    def test_set_get(self):
        x = helper.sample_minutes()

        symbol = 'TS123456'
        self.mstore[symbol] = x
        y = self.mstore[symbol]
        np.testing.assert_array_equal(y, x)

    def test_reopen(self):
        x = helper.sample_minutes()

        symbol = 'TS123456'
        self.mstore[symbol] = x

        # closed
        self.store.close()
        self.assertRaises(AssertionError, self.mstore.get, symbol)

        # reopen
        store = DictStore.open(self.filename)
        mstore = MinuteSnapshotCache(store, self.date)

        # testing reopen data
        y = mstore[symbol]
        np.testing.assert_array_equal(y, x)

    def test_rotate(self):
        x = helper.sample_minutes()

        symbol = 'TS123456'
        self.mstore[symbol] = x

        dbm = Manager(self.datadir, SH())
        tostore = dbm._minutestore_at(self.date, memory=False)

        # rewrite
        self.mstore.rotate(tostore)

        # cache cleaned after rotate
        self.assertRaises(KeyError, self.mstore.get, symbol)

        # testing persistent data
        y = tostore[symbol]
        np.testing.assert_array_equal(y, x)

        # reopen
        mstore = MinuteSnapshotCache(self.store, self.date)

        # testing reopen data
        self.assertRaises(KeyError, mstore.get, symbol)


class TestRockStaticPrefix(unittest.TestCase, TestHelper):

    def setUp(self):
        self._setup()
        self.db = datastore.RockStore.open(self.datadir)

    def tearDown(self):
        self._clean()

    def test_prefix(self):
        prefix = transform.int2bytes(2, 3)
        for x in range(3000):
            keyx = transform.int2bytes(x, 3) + b'.x'
            keyy = transform.int2bytes(x, 3) + b'.y'
            keyz = transform.int2bytes(x, 3) + b'.z'
            self.db.put(keyx, b'x')
            self.db.put(keyy, b'y')
            self.db.put(keyz, b'z')

        self.assertEqual(b'x', self.db.get(prefix + b'.x'))
        self.assertEqual(b'y', self.db.get(prefix + b'.y'))
        self.assertEqual(b'z', self.db.get(prefix + b'.z'))

        it = self.db.iterkeys(prefix=prefix)
        it.seek(prefix)

        ref = [prefix + b'.x', prefix + b'.y', prefix + b'.z']
        self.assertEqual(ref, list(it))


if __name__ == '__main__':
    unittest.main()
