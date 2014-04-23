from __future__ import with_statement

import h5py
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
from datafeed.exchange import SH
from datafeed.datastore import *
from datafeed.tests import helper

class TestHelper(object):
    def _clean(self):
        rpath = os.path.join(helper.datadir, 'rdb')
        shutil.rmtree(rpath, ignore_errors=True)

    def _close(self):
        self.manager.clean()


class ManagerTest(unittest.TestCase, TestHelper):

    def setUp(self):
        self._clean()
        self.manager = Manager(helper.datadir, SH())

    def tearDown(self):
        self._close()

    def test_store_filename(self):
        ret = self.manager._store
        self.assertEqual(ret.filename, '%s/data.h5' % helper.datadir)
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


class TickTest(unittest.TestCase):

    def test_init_store(self):
        filename = '%s/dstore.dump' % helper.datadir
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


class MinuteSnapshotCacheTest(unittest.TestCase):

    def setUp(self):
        self.filename = '%s/dstore_mincache.dump' % helper.datadir
        self.date = datetime.today().date()
        self.store = DictStore.open(self.filename)
        self.mstore = MinuteSnapshotCache(self.store, self.date)

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

        dbm = Manager(helper.datadir, SH())
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


if __name__ == '__main__':
    unittest.main()
    import shutil
    shutil.rmtree(helper.datadir)
