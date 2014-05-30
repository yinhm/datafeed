#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2010 yinhm

'''A datastore for quotes data management.

Datastore manager two types of data, one is archived data, they are stored at
one HDF5 file.

 * Day 1day OHLC
 * OneMinute: 1minute OHLC
 * FiveMinute: 5minute OHLC
 * HDF5 Minute: minute snapshot

Another type is data that has high update frequency, are stored at DictStore:

 * Report: snapshot of current price
 * MinuteSnapshotCache Minute: In session minute snapshots

There are two other stores: Dividend, Sector, we storing them to DictStore for
convenience.


Notice
======
Every market has diffrenct open/close time, etc. For now, datastore only
support one market, or several markets that have the same open/close time, then
you could distinguish them by prefix.
'''

import atexit
import datetime
import h5py
import logging
import marshal
import os
import time

import UserDict

import cPickle as pickle
import numpy as np


from datafeed.utils import *


__all__ = ['Manager', 'Minute', 'Day', 'OneMinute', 'FiveMinute',
           'DictStore', 'DictStoreNamespace', 'Report', 'MinuteSnapshotCache']

def date2key(date):
    '''Return formatted key from date.'''
    return date.strftime('%Y%m%d')


class Manager(object):
    '''Manager of datastores.

    It responsible for:
      * Managing different stores.
      * Dispatching read/write dataflows(this may change).
      * Rotating daily minutes snapshot.
   '''
    def __init__(self, datadir, exchange):
        self.datadir = datadir
        self.exchange = exchange

        logging.debug("Loading h5file and memory store...")
        self._store = h5py.File(os.path.join(self.datadir, 'data.h5'))
        self._dstore = DictStore.open(os.path.join(self.datadir, 'dstore.dump'))

        # Dict Store
        self._reportstore = None
        self._sectorstore = None
        self._divstore = None
        self._minutestore = None

        # HDF5 Store
        self._daystore = None
        self._1minstore = None
        self._5minstore = None

        self._mtime = None

        atexit.register(self.close)

    @property
    def divstore(self):
        '''Get dividend store instance or initialize if not present.
        '''
        if not self._divstore:
            self._divstore = Dividend(self._dstore)

        return self._divstore

    @property
    def reportstore(self):
        '''Get report instance or initialize if not present.

        :returns:
            Report instance.
        '''
        if not self._reportstore:
            logging.debug("Loading reports...")
            self._reportstore = Report(self._dstore)

        return self._reportstore

    @property
    def sectorstore(self):
        '''Get sector instance or initialize if not present.

        :returns:
            Report instance.
        '''
        if not self._sectorstore:
            logging.debug("Loading sectors...")
            self._sectorstore = Sector(self._dstore)

        return self._sectorstore

    @property
    def daystore(self):
        '''Get day instance or initialize if not present.

        :returns:
            Day instance.
        '''
        if not self._daystore:
            logging.info("Loading ohlcs...")
            self._daystore = Day(self._store)

        return self._daystore

    @property
    def minutestore(self):
        '''Return instance of last minutestore which should contains minute
        historical data.

        :returns:
            Minute instance.
        '''
        if not self._minutestore:
            logging.info("Loading minutestore at %d...", self.mtime)
            self._minutestore = self.get_minutestore_at(self.mtime,
                                                        memory=True)

        return self._minutestore

    def get_minutestore_at(self, timestamp, memory=None):
        """Get minutestore at given timestamp.

        If memory was not specified:

            * default to memory store for current date;
            * to file store for old date;
        """
        date = datetime.datetime.fromtimestamp(timestamp).date()
        if self._minutestore and \
                self._minutestore.date == date and \
                memory != False:
            return self._minutestore
        else:
            return self._minutestore_at(date, memory=memory)

    def _minutestore_at(self, date, memory=None):
        '''Return minute store at the given date.'''
        today = datetime.date.today()
        if memory or (memory == None and date == today):
            # Known issue:
            # Suppose server crashes, we restart it after couple of hours, then
            # we supply snapshots data, since minute store havn't rotated yet,
            # we may ends up with junk data, eg: SH600000 got suspended,
            # no fresh data for today, so minute store will holding some old
            # snapshots.
            f = MinuteSnapshotCache(self._dstore, date)
        else:
            f = self._store
        return Minute(f, date, self.exchange.market_minutes)

    @property
    def oneminstore(self):
        '''Get 1min ohlcs store instance or initialize if not present.

        :returns:
            OneMinute instance.
        '''
        if not self._1minstore:
            self._1minstore = OneMinute(self._store, self.exchange.market_minutes)

        return self._1minstore

    @property
    def fiveminstore(self):
        '''Get 5min ohlcs store instance or initialize if not present.

        :returns:
            FiveMinute instance.
        '''
        if not self._5minstore:
            self._5minstore = FiveMinute(self._store, self.exchange.market_minutes)

        return self._5minstore

    def rotate_minute_store(self):
        ''' Rotate minute store when new trading day data flowed in.
        '''
        date = datetime.datetime.fromtimestamp(self.mtime).date()
        if self._minutestore and date != self._minutestore.date:
            logging.info("==> Ratate minute store...")
            # _minutestore was always stored in cache,
            # we need to rewrite it to Minute store for persistent.
            tostore = self._minutestore_at(self._minutestore.date, memory=False)
            self._minutestore.store.rotate(tostore)
            self._minutestore = None

        return self.minutestore

    @property
    def mtime(self):
        "Modify time, updated we report data received."
        return self._mtime

    def set_mtime(self, ts):
        if ts > self.mtime:
            self._mtime = ts
    
    @property
    def last_quote_time(self):
        logging.warning("Deprecated, using mtime instead.")
        return self.mtime
    
    def get_report(self, symbol):
        """Get report by symbol."""
        return self.reportstore[symbol]
        
    def get_reports(self, *args):
        """Get reports by symbols.

        Return:
          dict iterator
        """
        if len(args) > 0:
            store = self.reportstore
            ret = dict([(symbol, store.get(symbol)) for symbol in args if store.has_key(symbol) ])
        else:
            ret = self.reportstore.iteritems()
            
        return ret

    def update_reports(self, data):
        if len(data) == 0:
            return
        time = data[data.keys()[0]]['timestamp']
        self.set_mtime(time)
        self.reportstore.update(data)

    def update_minute(self, symbol, data):
        # determine datastore first
        for minute in data:
            timestamp = minute['time']
            break
        store = self.get_minutestore_at(timestamp)
        store.update(symbol, data)
    
    def update_day(self, symbol, data):
        self.daystore.update(symbol, data)

    def update_dividend(self, symbol, data):
        if len(data) == 0:
            return
        
        try:
            self.divstore[symbol] = data
        except ValueError:
            del self.divstore[symbol]
            self.divstore[symbol] = data

    def close(self):
        logging.debug("datastore shutdown, saving data.")
        self._dstore.close()


class DictStore(dict):

    def __init__(self, filename, odict):
        self.filename = filename
        self.closed = False
        super(DictStore, self).__init__(odict)

    def require_group(self, key):
        if not self.has_key(key):
            self.__setitem__(key, dict())
        return self.__getitem__(key)

    @classmethod
    def open(cls, filename):
        data = {}
        if os.path.exists(filename):
            data = pickle.load(open(filename, 'rb'))
        return cls(filename, data)

    def close(self):
        self.flush()
        self.closed = True

    def flush(self):
        pickle.dump(self.items(), open(self.filename, 'wb+'), -1)


class DictStoreNamespace(object, UserDict.DictMixin):
    def __init__(self, store):
        self.store = store
        klass = self.__class__.__name__
        if klass == 'DictStoreNamespace':
            raise StandardError("Can not initialize directly.")
        self.handle = store.require_group(klass.lower())

    def __repr__(self):
        return '%s(...)' % self.__class__.__name__

    def to_dict(self):
        return self.handle

    def flush(self):
        self.store.flush()

    def keys(self):
        assert not self.store.closed
        return self.handle.keys()

    def __len__(self):
        assert not self.store.closed
        return len(self.handle)

    def __nonzero__(self):
        "Truth value testing, always return True."
        assert not self.store.closed
        return True

    def has_key(self, key):
        assert not self.store.closed
        return key in self.handle

    def set(self, key, value):
        self.__setitem__(key, value)

    def get(self, key):
        return self.__getitem__(key)

    def __setitem__(self, key, value):
        assert not self.store.closed
        self.handle.__setitem__(key, value)

    def __getitem__(self, key):
        assert not self.store.closed
        return self.handle.__getitem__(key)

    def __delitem__(self, key):
        assert not self.store.closed
        return self.handle.__delitem__(key)

class Report(DictStoreNamespace):
    pass

class Sector(DictStoreNamespace):
    pass

class Dividend(DictStoreNamespace):
    pass


class OHLC(object):
    '''OHLC data archive.'''

    DTYPE = np.dtype({'names': ('time', 'open', 'high', 'low', 'close', 'volume', 'amount'),
                      'formats': ('i4', 'f4', 'f4', 'f4', 'f4', 'f4', 'f4')})

    time_interval = 60 # default to 60 seconds(1min)
    _handle = None

    def __init__(self, store, market_minutes=None):
        '''Init day store from handle.

        Handle should be in each implementors namespace, eg:

          day: /day
          1min: /1min
          5min: /5min
        '''
        self.store = store

        self.shape_x = None
        self.market_minutes = market_minutes

        if market_minutes:
            self.shape_x = market_minutes / (self.time_interval / 60)

    def __nonzero__(self):
        "Truth value testing."
        return True

    @property
    def handle(self):
        raise StandardError("No implementation.")

    def flush(self):
        self.handle.file.flush()

    def get(self, symbol, date):
        """Get minute history quote data for a symbol.

        Raise:
          KeyError: if symbol not exists.

        Return:
          numpy data
        """
        key = self._key(symbol, date)
        return self.handle[key][:]

    def update(self, symbol, quotes):
        """Archive daily ohlcs, override if datasets exists."""
        assert quotes['time'][0] < quotes['time'][1], \
            'Data are not chronological ordered.'

        # FIXME: disable update_multi for markets like SH
        # need to fix timestamp_to_index first
        if self.market_minutes == 1440: # full day
            self._update_multi(symbol, quotes)
        else:
            self._update(symbol, quotes)

    def _update(self, symbol, quotes):
        """Archive daily ohlcs, override if datasets exists.

        Arguments:
          symbol: Stock instrument.
          quotes: numpy quotes data.
        """
        i = 0
        pre_ts = 0
        indexes = []
        for q in quotes:
            # 2 hours interval should be safe for seperate daily quotes
            if pre_ts and (q['time'] - pre_ts) > 7200:
                indexes.append(i)
            pre_ts = q['time']
            i += 1
        indexes.append(i)

        pre_index = 0
        for i in indexes:
            sliced_qs = quotes[pre_index:i]
            date = datetime.datetime.fromtimestamp(sliced_qs[0]['time']).date()
            try:
                ds = self._require_dataset(symbol, date, sliced_qs.shape)
            except TypeError, e:
                if e.message.startswith('Shapes do not match'):
                    self._drop_dataset(symbol, date)
                    ds = self._require_dataset(symbol, date, sliced_qs.shape)
                else:
                    raise e
            ds[:] = sliced_qs
            pre_index = i

    def _update_multi(self, symbol, quotes):
        """Archive multiday ohlcs, override if datasets exists.

        Arguments:
          symbol: Stock instrument.
          quotes: numpy quotes data.
        """
        i = 0
        pre_day = None
        indexes = []
        indexes.append([0, len(quotes)])
        for row in quotes:
            day = datetime.datetime.fromtimestamp(row['time']).day
            if pre_day and pre_day != day:
                # found next day boundary
                indexes[-1][1] = i
                indexes.append([i, len(quotes)])
            i += 1
            pre_day = day

        for i0, i1 in indexes:
            t0, t1 = quotes[i0]['time'], quotes[i1-1]['time']
            dt = datetime.datetime.fromtimestamp(t0)
            dsi0, dsi1 = self.timestamp_to_index(dt, t0), self.timestamp_to_index(dt, t1)

            sliced = quotes[i0:i1]
            ds = self._require_dataset(symbol, dt.date(), sliced.shape)

            if dsi0 != 0:
                dsi1 = dsi1 + 1
            logging.debug("ds[%d:%d] = quotes[%d:%d]" % (dsi0, dsi1, i0, i1))
            try:
                ds[dsi0:dsi1] = sliced
            except TypeError:
                logging.debug("data may have holes")
                for row in sliced:
                    r_dsi = self.timestamp_to_index(dt, row['time'])
                    # logging.debug("r_dsi: %d" % r_dsi)
                    ds[r_dsi] = row

    def timestamp_to_index(self, dt, ts):
        day_start = time.mktime((dt.year, dt.month, dt.day,
                                 0, 0, 0, 0, 0, 0))
        return int((ts - day_start) / self.time_interval)

    def _require_dataset(self, symbol, date, shape=None):
        '''Require dateset for a specific symbol on the given date.'''
        assert self.shape_x or shape

        key = self._key(symbol, date)
        if self.shape_x:
            shape = (self.shape_x, )
        return self.handle.require_dataset(key,
                                           shape,
                                           dtype=self.DTYPE)

    def _drop_dataset(self, symbol, date):
        '''Require dateset for a specific symbol on the given date.'''
        key = self._key(symbol, date)
        del(self.handle[key])


    def _key(self, symbol, date):
        '''Format key path.'''
        # datetime.datetime.fromtimestamp(timestamp).date()
        return "%s/%s" % (symbol, date2key(date))


class Day(OHLC):
    '''Archive of daily OHLCs data.

    OHLCs are grouped by symbol and year:

        SH000001/2009
        SH000001/2010
        SH000002/2009
        SH000002/2010
    '''

    # ISO 8601 year consists of 52 or 53 full weeks.
    # See: http://en.wikipedia.org/wiki/ISO_8601
    WORKING_DAYS_OF_YEAR = 53 * 5
    
    @property
    def handle(self):
        if not self._handle:
            self._handle = self.store.require_group('day')
        return self._handle

    def get(self, symbol, length):
        year = datetime.datetime.today().isocalendar()[0]
        try:
            data = self._get_year_data(symbol, year)
        except KeyError:
            self.handle[symbol] # test symbol existence
            data = []

        while True:
            if len(data) >= length:
                break
            
            year = year - 1
            try:
                ydata = self._get_year_data(symbol, year)
            except KeyError:
                # wrong length
                return data
            
            if len(ydata) == 0:
                break

            if len(data) == 0:
                data = ydata
            else:
                data = np.append(ydata, data)
        return data[-length:]

    def get_by_date(self, symbol, date):
        year = date.isocalendar()[0]
        ds = self._dataset(symbol, year)
        index = self._index_of_day(date)
        return ds[index]

    def update(self, symbol, data):
        """append daily history data to daily archive.

        Arguments
        =========
        - `symbol`: symbol.
        - `npydata`: data of npy file.
        """
        prev_year = None
        ds = None
        newdata = None

        for row in data:
            day = datetime.datetime.fromtimestamp(row['time'])
            isoyear = day.isocalendar()[0]

            if prev_year != isoyear:
                if prev_year:
                    # ds will be changed, save prev ds first
                    ds[:] = newdata
                ds = self._require_dataset(symbol, isoyear)
                newdata = ds[:]
            index = self._index_of_day(day)
            try:
                newdata[index] = row
            except IndexError, e:
                logging.error("IndexError on: %s, %s, %s" % (symbol, isoyear, day))
            prev_year = isoyear

        if ds != None and newdata != None:
            ds[:] = newdata

        self.flush()
        return True
    
    def _get_year_data(self, symbol, year):
        ds = self._dataset(symbol, year)
        data = ds[:]
        return data[data.nonzero()]

    def _index_of_day(self, day):
        '''Determing index by day from a dataset.

        We index and store market data by each working day.
        '''
        year, isoweekday, weekday = day.isocalendar()
        return (isoweekday - 1) * 5 + weekday - 1

    def _dataset(self, symbol, year):
        '''We store year of full iso weeks OHLCs in a dataset.

        eg, 2008-12-29 is iso 8061 "2009-W01-1",
        the OHLC data are stored in "symbol/2009/[index_of_0]"
        '''
        return self.handle[self._key(symbol, year)]

    def _require_dataset(self, symbol, year):
        '''Like _dataset, but create on KeyError.'''
        key = self._key(symbol, year)
        return self.handle.require_dataset(key,
                                           (self.WORKING_DAYS_OF_YEAR, ),
                                           dtype=self.DTYPE)

    def _key(self, symbol, year):
        return '%s/%s' % (symbol, str(year))


class OneMinute(OHLC):
    '''Archive of daily 1 minute ohlcs.

    Grouped by symbol and date:

        1min/SH000001/20090101
        1min/SH000001/20090102
    '''

    @property
    def handle(self):
        if not self._handle:
            self._handle = self.store.require_group('1min')
        return self._handle
        

class FiveMinute(OHLC):
    '''Archive of daily 5 minute ohlcs.

    Grouped by symbol and date:

        5min/SH000001/20090101
        5min/SH000001/20090102
    '''
    time_interval = 5 * 60

    @property
    def handle(self):
        if not self._handle:
            self._handle = self.store.require_group('5min')
        return self._handle


class Minute(object):
    '''Snapshot of daily minute quotes history.
    '''
    DTYPE = np.dtype({'names': ('time', 'price', 'volume', 'amount'),
                      'formats': ('i4', 'f4', 'f4', 'f4')})

    def __init__(self, store, date, shape_x):
        assert isinstance(date, datetime.date)
        logging.info("==> Load %s at %s" % (str(store), str(date)))
        self.store = store
        self.date = date
        # TBD: minsnap/date will created on init, this may end with junk datasets
        self.handle = self.store.require_group('minsnap/%s' % date2key(date))
        self.shape_x = shape_x

    @property
    def filename(self):
        return self.handle.filename

    @property
    def pathname(self):
        return self.handle.name

    def flush(self):
        self.handle.file.flush()

    def keys(self):
        "Return a list of keys in archive."
        return self.handle.keys()
        
    def values(self):
        "Return a list of objects in archive."
        return self.handle.values()
        
    def items(self):
        "Return a list of all (key, value) pairs."
        return self.handle.items()

    def iterkeys(self):
        "An iterator over the keys."
        return self.handle.itemkeys()

    def itervalues(self):
        "An iterator over the values."
        return self.handle.itemvalues()

    def iteritems(self):
        "An iterator over (key, value) items."
        return self.handle.iteritems()

    def has_key(self, key):
        "True if key is in archive, False otherwise."
        return key in self.handle

    def get(self, symbol):
        """Get minute history quote data for a symbol.

        Raise:
          KeyError: if symbol not exists.

        Return:
          numpy data
        """
        return self[symbol]

    def set(self, symbol, index, data):
        """Set minute history quote data for a symbol.
        """
        ds = self._require_dataset(symbol)
        ds[index] = data

    def update(self, symbol, data):
        """Update minute snapshots.
        
        Arguments:
          symbol`: Stock instrument.
          data`: numpy quotes data.
        """
        day = datetime.datetime.fromtimestamp(data[0]['time']).date()
        assert day == self.date
        self[symbol] = data

    def __iter__(self):
        return iter(self.keys())

    def __len__(self):
        return len(self.keys())

    def __nonzero__(self):
        "Truth value testing, always return True."
        return True

    def __getitem__(self, key):
        ds = self._dataset(key)
        return ds[:]

    def __setitem__(self, key, value):
        ds = self._require_dataset(key)
        if len(value) > self.shape_x:
            # TODO: filter data before exchange open time.
            # @FIXME seems data begin from 9:15, so we have 253 instead of 242
            # this breaks datastore cause we assuming day data length should be
            # 242
            value = value[-self.shape_x:]
            ds[:] = value
        elif len(value) < self.shape_x:
            ds[:len(value)] = value
        else:
            ds[:] = value
        return True

    def __delitem__(self, key):
        ds = self._dataset(key)
        del self.handle[ds.name]

    def _dataset(self, symbol):
        return self.handle[symbol]

    def _require_dataset(self, symbol):
        try:
            return self._dataset(symbol)
        except KeyError:
            return self.handle.create_dataset(symbol,
                                              (self.shape_x, ),
                                              self.DTYPE)


class MinuteSnapshotCache(DictStoreNamespace):
    '''Mock for basic h5py interface.

    This is only used to enhance performance of minute store.
    '''
    def __init__(self, store, date):
        assert isinstance(store, DictStore)
        assert isinstance(date, datetime.date)

        super(MinuteSnapshotCache, self).__init__(store)
        self.date = date


    def __repr__(self):
        return self.__class__.__name__

    def __str__(self):
        return self.__repr__()

    @property
    def name(self):
        '''Compatible with hdf5 for tests.'''
        return '/minsnap/%s' % self.pathname

    @property
    def filename(self):
        return self.store.filename

    @property
    def pathname(self):
        return date2key(self.date)

    @property
    def file(self):
        return self

    def require_group(self, gid):
        """Cache dict store act as one group.
        """
        return self

    def create_dataset(self, symbol, shape, dtype):
        self.__setitem__(symbol, np.zeros(shape, dtype))
        return self.__getitem__(symbol)

    def rotate(self, tostore):
        assert not isinstance(tostore.store, MinuteSnapshotCache)
        assert tostore.date == self.date

        logging.info("==> Rotating %s min snapshots." % self.pathname)
        self._rewrite(tostore)

    def _rewrite(self, tostore):
        if self.__len__() > 0:
            for key in self.keys():
                try:
                    tostore.update(key, self.__getitem__(key))
                except AssertionError:
                    logging.error("Inconsistent data for %s, ignoring." % key)
                self.__delitem__(key)
            tostore.flush()
