from __future__ import with_statement

import datetime
import re
import time
import unittest

from datafeed.exchange import SH
from datafeed.imiguserver import ImiguApplication, ImiguHandler, SnapshotIndexError
from datafeed.server import Request
from datafeed.tests import helper

from mock import Mock, patch


class ImiguApplicationTest(unittest.TestCase):

    def setUp(self):
        self.application = ImiguApplication(helper.datadir, SH())
        self.application.dbm._mtime = 1291167000
        self.open_time = 1291167000
        self.close_time = 1291186800

        key = helper.sample_key()
        sample = helper.sample()
        sample[key]['timestamp'] = 1291167000
        self.application.dbm.reportstore.update(sample)


    @patch.object(time, 'time')
    def test_archive_day_09_29(self, mock_time):
        mock_time.return_value = self.open_time - 1 # not open

        today = datetime.datetime.today()
        ret = self.application.scheduled_archive_day(today)
        self.assertFalse(ret)

    @patch.object(time, 'time')
    def test_archive_day_15_05_no_data(self, mock_time):
        mock_time.return_value = self.close_time + 300
        
        self.application.dbm._mtime = self.close_time - 86400

        today = datetime.datetime.today()
        ret = self.application.scheduled_archive_day(today)
        self.assertFalse(ret)

    @patch.object(time, 'time')
    def test_archive_day_15_05_01(self, mock_time):
        mock_time.return_value = self.close_time + 181 # closed more than 3 minutes

        self.application.dbm._mtime = self.close_time + 180 + 1

        today = datetime.datetime.today()
        ret = self.application.scheduled_archive_day(today)
        self.assertTrue(ret)

    @patch.object(time, 'time')
    def test_archive_day_15_05_01_archived_before(self, mock_time):
        mock_time.return_value = self.close_time + 181 # closed more than 3 minutes

        self.application.archive_day_time = self.close_time + 180

        today = datetime.datetime.today()
        ret = self.application.scheduled_archive_day(today)
        self.assertFalse(ret)

    @patch.object(time, 'time')
    def test_archive_minute_09_29(self, mock_time):
        mock_time.return_value = self.open_time - 1 # before open

        today = datetime.datetime.today()
        ret = self.application.scheduled_archive_minute(today)
        self.assertFalse(ret)

    @patch.object(time, 'time')
    def test_archive_minute_09_30(self, mock_time):
        mock_time.return_value = self.open_time

        today = datetime.datetime.today()
        ret = self.application.scheduled_archive_minute(today)
        self.assertTrue(ret)

    @patch.object(time, 'time')
    def test_archive_minute_14_30(self, mock_time):
        mock_time.return_value = self.close_time - 1800 # in session

        today = datetime.datetime.today()
        ret = self.application.scheduled_archive_minute(today)
        self.assertTrue(ret)

    @patch.object(time, 'time')
    def test_archive_minute_14_30_05_if_not_archived(self, mock_time):
        mock_time.return_value = self.close_time - 1795 # in session

        self.application.archive_minute_time = self.close_time - 1860

        today = datetime.datetime.today()
        ret = self.application.scheduled_archive_minute(today)
        self.assertTrue(ret)

    @patch.object(time, 'time')
    def test_archive_minute_14_30_05_if_archived(self, mock_time):
        mock_time.return_value = self.close_time - 1795 # in session

        self.application.archive_minute_time = self.close_time - 1800

        today = datetime.datetime.today()
        ret = self.application.scheduled_archive_minute(today)
        self.assertFalse(ret)

    @patch.object(time, 'time')
    def test_archive_minute_15_00(self, mock_time):
        mock_time.return_value = self.close_time

        today = datetime.datetime.today()
        ret = self.application.scheduled_archive_minute(today)
        self.assertTrue(ret)

    @patch.object(time, 'time')
    def test_archive_minute_15_03(self, mock_time):
        mock_time.return_value = self.close_time + 180

        today = datetime.datetime.today()
        ret = self.application.scheduled_archive_minute(today)
        self.assertTrue(ret)

    @patch.object(time, 'time')
    def test_archive_minute_15_05_01(self, mock_time): 
        mock_time.return_value = self.close_time + 300 + 1 # closed

        today = datetime.datetime.today()
        ret = self.application.scheduled_archive_minute(today)
        self.assertFalse(ret)

    @patch.object(time, 'time')
    def test_crontab_08_00_00(self, mock_time): 
        mock_time.return_value = self.open_time - 3600 - 1800

        today = datetime.datetime.fromtimestamp(time.time())
        ret = self.application.scheduled_crontab_daily(today)
        self.assertTrue(ret)

    @patch.object(time, 'time')
    def test_crontab_08_00_01_if_not_running(self, mock_time): 
        mock_time.return_value = self.open_time - 3600 - 1799

        self.application.crontab_time = self.open_time - 86400 - 7200
        today = datetime.datetime.fromtimestamp(time.time())
        ret = self.application.scheduled_crontab_daily(today)
        self.assertTrue(ret)

    @patch.object(time, 'time')
    def test_crontab_09_30(self, mock_time): 
        mock_time.return_value = self.open_time

        today = datetime.datetime.fromtimestamp(time.time())
        ret = self.application.scheduled_crontab_daily(today)
        self.assertFalse(ret)

    def test_archive_day(self):
        r = {
            'amount': 84596203520.0,
            'close': 2856.9899999999998,
            'high': 2880.5599999999999,
            'low': 2851.9499999999998,
            'name': u'\u4e0a\u8bc1\u6307\u6570',
            'open': 2868.73,
            'preclose': 2875.8600000000001,
            'price': 2856.9899999999998,
            'symbol': 'SH000001',
            'volume': 75147848.0
            }
        
        day = datetime.datetime.today()
        ts = time.mktime((day.year, day.month, day.day,
                          15, 0, 0, 0, 0, 0))
        day_ts = time.mktime((day.year, day.month, day.day,
                              0, 0, 0, 0, 0, 0))
        r['timestamp'] = ts
        r['time'] = str(datetime.datetime.fromtimestamp(ts))

        data = {'SH000001': r}

        import zlib
        import marshal
        data = zlib.compress(marshal.dumps(data))
        
        request = Request(None, 'put_reports', data)
        self.application(request)

        request = Request(None, 'archive_day')
        self.application(request)
        
        y = self.application.dbm.daystore.get('SH000001', 1)
        self.assertEqual(y[0]['time'], day_ts)
        self.assertTrue((y[0]['open'] - 2868.73) < 0.1 ** 6)

    @patch.object(ImiguHandler, 'get_snapshot_index')
    def test_fix_report_when_archive(self, mock_index):
        # set to after hours: 15:30 implicates error data
        # some datafeed still sending data even market was closed.
        day = datetime.datetime.today()
        ts = time.mktime((day.year, day.month, day.day,
                          15, 30, 0, 0, 0, 0))
        mock_index.return_value = (ts, 360)
        
        r = {
            'amount': 84596203520.0,
            'close': 2856.9899999999998,
            'high': 2880.5599999999999,
            'low': 2851.9499999999998,
            'name': u'\u4e0a\u8bc1\u6307\u6570',
            'open': 2868.73,
            'preclose': 2875.8600000000001,
            'price': 2856.9899999999998,
            'symbol': 'SH000001',
            'time': '2010-12-08 14:02:57',
            'timestamp': 1291788177,
            'volume': 75147848.0
            }
        

        r['timestamp'] = ts
        r['time'] = str(datetime.datetime.fromtimestamp(ts))

        data = {'SH000001': r}

        import zlib
        import marshal
        data = zlib.compress(marshal.dumps(data))
        
        request = Request(None, 'put_reports', data)
        self.application(request)

        close_time = time.mktime((day.year, day.month, day.day,
                                  15, 0, 0, 0, 0, 0))
        
        request = Request(None, 'archive_minute', data)
        self.application(request)
        
        r = self.application.dbm.get_report('SH000001')
        self.assertEqual(r['timestamp'], close_time)
        self.assertEqual(r['open'], 2868.73)

    @patch.object(ImiguHandler, 'get_snapshot_index')
    def test_archive_minute_at_open_time(self, mock_index):
        # set data time to pre-market(centralized competitive pricing)
        day = datetime.datetime.today()
        t1 = time.mktime((day.year, day.month, day.day,
                          9, 26, 0, 0, 0, 0))
        open_time = time.mktime((day.year, day.month, day.day,
                                 9, 30, 0, 0, 0, 0))
        mock_index.return_value = (open_time, 0)
        
        r = {
            'amount': 10000.0,
            'close': 0.0,
            'high': 3000.0,
            'low': 3000.0,
            'name': u'\u4e0a\u8bc1\u6307\u6570',
            'open': 3000.0,
            'preclose': 2875.0,
            'price': 3000.0,
            'symbol': 'SH000001',
            'volume': 900000.0
            }
        
        r['timestamp'] = t1
        r['time'] = str(datetime.datetime.fromtimestamp(t1))

        data = {'SH000001': r}

        import zlib
        import marshal
        data = zlib.compress(marshal.dumps(data))
        
        request = Request(None, 'put_reports', data)
        self.application(request)

        self.assertEqual(self.application.dbm.mtime, t1)
        
        request = Request(None, 'archive_minute')
        self.application(request)
        
        y = self.application.dbm.minutestore.get('SH000001')
        self.assertEqual(y[0]['time'], open_time)
        self.assertEqual(y[0]['price'], 3000.0)

    @patch.object(ImiguHandler, 'get_snapshot_index')
    def test_archive_minute_raise_at_wrong_index(self, mock_index):
        # set data time to pre-market(centralized competitive pricing)
        day = datetime.datetime.today()
        t1 = time.mktime((day.year, day.month, day.day,
                          9, 26, 0, 0, 0, 0))
        mock_index.return_value = (t1, -4)

        request = Request(None, 'archive_minute')
        self.assertRaises(SnapshotIndexError,
                          self.application,
                          request)

    @patch.object(time, 'time')
    def test_get_snapshot_index(self, mock_time):
        mock_time.return_value = 1309829400
        report_time = 1309829160

        mintime, index = ImiguHandler.get_snapshot_index(1309829400, report_time)

        self.assertEqual(mintime, 1309829400)
        self.assertEqual(index, 0)


if __name__ == '__main__':
    unittest.main()
