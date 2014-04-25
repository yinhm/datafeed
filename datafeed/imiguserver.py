#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2011 yinhm

'''Imigu.com specific datafeed server implementation.
'''

import datetime
import logging
import time

import numpy as np

from datafeed.providers.dzh import DzhDividend, DzhSector
from datafeed.server import *
from datafeed.utils import *


__all__ = ['ImiguApplication', 'ImiguHandler']


class SnapshotIndexError(KeyError):
    pass



class ImiguApplication(Application):

    def __init__(self, datadir, exchange, **kwds):
        self.archive_minute_time = 0
        self.archive_day_time = 0
        self.crontab_time = 0
        
        self._tasks = []
        
        super(ImiguApplication, self).__init__(datadir, exchange, handler=ImiguHandler, **kwds)

        # last quote time reset to SH000001's timestamp
        try:
            r = self.dbm.get_tick("SH000001")
            self.dbm.set_mtime(r['timestamp'])
        except KeyError:
            self.dbm.set_mtime(time.time())

    def periodic_job(self):
        today = datetime.datetime.today()

        if self.scheduled_archive_minute(today):
            request = Request(None, 'archive_minute')
            self.__call__(request)

        if self.scheduled_archive_day(today):
            request = Request(None, 'archive_day')
            self.__call__(request)

        if self.scheduled_crontab_daily(today):
            request = Request(None, 'crontab_daily')
            self.__call__(request)

        if len(self._tasks) > 0:
            logging.info("tasks left: %s" % len(self._tasks))
            request = Request(None, 'run_task')
            self.__call__(request)

    def scheduled_archive_minute(self, today):
        """Test is archive minute scheduled.
        """
        now = time.time()

        market_open_at = self.exchange.open_time(now=now)
        if now < market_open_at:
            # Should not archive any data if market not open yet.
            logging.debug("market not open yet")
            return False        

        market_closing_at = self.exchange.close_time(now=now)
        if now > (market_closing_at + 60 * 5):
            # Do not archive if time passed 15:05.
            # Should be archived already. If not, something is broken.
            logging.debug("market closed more than 5 minutes")
            return False

        # quote_time = self.dbm.mtime
        # if (now - quote_time) > 60:
        #     return False

        # in session, we run it every 60 sec or greater
        if today.second == 0 or (now - self.archive_minute_time) > 60:
            return True

        return False

    def scheduled_archive_day(self, today):
        """Test is daily archive scheduled.
        """
        now = time.time()
        close_time = self.exchange.close_time(now=now)

        if now < close_time:
            logging.debug("market not closed yet.")
            return False

        if self.dbm.mtime < close_time:
            logging.debug("No market data: Weekday or holiday or datafeed receiver broken.")
            return False

        if self.dbm.mtime < self.archive_day_time:
            logging.debug("Already archived.")
            return False

        # skip 60 * 3 sec make sure we got the last data
        if now > (close_time + 60 * 3):
            return True

        return False

    def scheduled_crontab_daily(self, today):
        """Test is daily crontab scheduled.
        """
        if today.hour == 8:
            if today.minute == 0 and today.second == 0:
                return True
            
            now = time.time()
            if today.minute == 0 and (now - self.crontab_time) > 86400:
                # not runned before
                return True

        return False

    def task_add(self, task):
        self._tasks.append(task)
    
    def task_reserve(self):
        return self._tasks.pop(0)
    
class ImiguHandler(Handler):

    SUPPORTED_METHODS = Handler.SUPPORTED_METHODS + \
        ('archive_day',
         'archive_minute',
         'crontab_daily',
         'sync_dividend',
         'sync_sector',
         'run_task')


    ###### periodic jobs ######
    
    def archive_day(self, *args):
        """Archive daily data from tick datastore.
        """
        dt = datetime.datetime.fromtimestamp(self.dbm.mtime).date()

        store = self.dbm.daystore
        ticks = self.dbm.get_ticks()
        for symbol, tick in ticks:
            if 'timestamp' not in tick:
                continue

            d = datetime.datetime.fromtimestamp(tick['timestamp'])

            if dt != d.date():
                # skip instruments which no recent tick data
                continue
            
            t = int(time.mktime(d.date().timetuple()))

            row = (t, tick['open'], tick['high'], tick['low'],
                   tick['close'], tick['volume'], tick['amount'])
            
            data = np.array([row], dtype=store.DTYPE)
            store.update(symbol, data)
        
        self.application.archive_day_time = time.time()
        logging.info("daily data archived.")

        if self.request.connection:
            self.request.write("+OK\r\n")

    def archive_minute(self, *args):
        '''Archive minute data from tick datastore.
        '''
        logging.info("starting archive minute...")
        self.application.archive_minute_time = time.time()

        dbm = self.dbm
        pre_open_time = dbm.exchange.pre_open_time(now=dbm.mtime)
        open_time = dbm.exchange.open_time(now=dbm.mtime)
        break_time = dbm.exchange.break_time(now=dbm.mtime)
        close_time = dbm.exchange.close_time(now=dbm.mtime)

        try:
            tick = dbm.get_tick('SH000001')
            rts = tick['timestamp']
        except KeyError:
            logging.error("No SH000001 data.")
            if not self.request.connection:
                return
            return self.request.write("-ERR No data yet.\r\n")

        if rts < pre_open_time:
            logging.error("wrong tick time: %s." % \
                              (datetime.datetime.fromtimestamp(rts), ))
            if not self.request.connection:
                return
            return self.request.write("-ERR No data yet.\r\n")

        mintime, index = ImiguHandler.get_snapshot_index(open_time, rts)

        if index < 0:
            raise SnapshotIndexError

        # Rotate when we sure there is new data coming in.
        dbm.rotate_minute_store()
        store = dbm.minutestore

        snapshot_time = mintime
        cleanup_callback = lambda r: r
        if index > 120 and index < 210:
            # sometimes we received tick within 11:31 - 12:59
            # reset to 11:30
            snapshot_time = break_time
            def cleanup_callback(r):
                r['timestamp'] = break_time
                r['time'] = str(datetime.datetime.fromtimestamp(break_time))

            index = 120
        elif index >= 210 and index <= 330:
            index = index - 89  # subtract 11:31 - 12:59
        elif index > 330:
            # sometimes we received tick after 15:00
            # reset to 15:00
            snapshot_time = close_time
            def cleanup_callback(r):
                r['timestamp'] = close_time
                r['time'] = str(datetime.datetime.fromtimestamp(close_time))

            index = 241

        ticks = dbm.get_ticks()
        for key, tick in ticks:
            if 'timestamp' not in tick:
                # Wrong data
                continue

            if mintime - tick['timestamp'] > 1800:
                # no new data in 30 mins, something broken
                # skip this symbol when unknown
                continue

            cleanup_callback(tick)
            
            mindata = (snapshot_time, tick['price'], tick['volume'], tick['amount'])
            y = np.array(mindata, dtype=store.DTYPE)

            store.set(key, index, y)

        #store.flush()

        logging.info("snapshot to %i (index of %i)." % (mintime, index))
        self.request.write_ok()

    @classmethod
    def get_snapshot_index(cls, open_time, tick_time):
        ts = time.time()
        d = datetime.datetime.fromtimestamp(ts)
        mintime = time.mktime((d.year, d.month, d.day,
                               d.hour, d.minute,
                               0, 0, 0, 0))
        index = int((mintime - open_time) / 60)
        logging.info("minute data at %i (index of %i)." % (mintime, index))
        return (int(mintime), index)
                          
    def crontab_daily(self, *args):
        self.application.crontab_time = time.time()
        self.sync_dividend()
        self.sync_sector()

    def sync_dividend(self, *args):
        io = DzhDividend()
        for symbol, data in io.read():
            self.dbm.update_dividend(symbol, data)
        self.dbm.divstore.flush()
        self.request.write_ok()

    def sync_sector(self, *args):
        io = DzhSector()
        for sector, options in io.read():
            self.dbm.sectorstore[sector] = options
        self.request.write_ok()

    def run_task(self):
        for i in xrange(300):
            try:
                task = self.application.task_reserve()
            except IndexError:
                break
            task.run()
        

class Task(object):
    __slots__ = ['store', 'key', 'index', 'data']
    
    def __init__(self, store, key, index, data):
        self.store = store
        self.key = key
        self.index = index
        self.data = data

    def run(self):
        self.store.set(self.key, self.index, self.data)
