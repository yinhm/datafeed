#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2011 yinhm

import datetime
import numpy as np

from pandas import DataFrame
from pandas import TimeSeries
from pandas import DatetimeIndex

    
class Dividend(object):

    def __init__(self, div):
        """
        Paramaters:
          div: numpy dividend data.
        """
        assert div['time'] > 0
        assert abs(div['split']) > 0 or \
            abs(div['purchase']) > 0 or \
            abs(div['dividend']) > 0

        self._npd = div

    def adjust(self, frame):
        '''Adjust price, volume of quotes data.
    
        Paramaters
        ----------
        frame: DataFrame of OHLCs.
        '''
        if self.ex_date <= frame.index[0].date(): # no adjustment needed
            return True

        self._divide(frame)
        self._split(frame)

    def _divide(self, frame):
        if self.cash_afterward == 0:
            return

        cashes = [self.cash_afterward, 0.0]
        adj_day = self.ex_date - datetime.timedelta(days=1)
        indexes = []
        indexes.append(self.d2t(adj_day))
        indexes.append(self.d2t(datetime.date.today()))
        
        cashes = TimeSeries(cashes, index=indexes)
        ri_cashes = cashes.reindex(frame.index, method='backfill')

        frame['adjclose'] = frame['adjclose'] - ri_cashes

    def _split(self, frame):
        if self.share_afterward == 1:
            return

        splits = [self.share_afterward, 1.0]
        adj_day = self.ex_date - datetime.timedelta(days=1)
        indexes = []
        indexes.append(self.d2t(adj_day))
        indexes.append(self.d2t(datetime.date.today()))
        
        splits = TimeSeries(splits, index=indexes)
        ri_splits = splits.reindex(frame.index, method='backfill')

        frame['adjclose'] = frame['adjclose'] / ri_splits

    @property
    def ex_date(self):
        return datetime.date.fromtimestamp(self._npd['time'])

    @property
    def cash_afterward(self):
        return self._npd['dividend'] - self._npd['purchase'] * self._npd['purchase_price']
        
    @property
    def share_afterward(self):
        return 1 + self._npd['purchase'] + self._npd['split']

    def d2t(self, date):
        return datetime.datetime.combine(date, datetime.time())


def adjust(y, divs, capitalize=False):
    """Return fully adjusted OHLCs data base on dividends

    Paramaters:
    y: numpy
    divs: numpy of dividends

    Return:
    DataFrame objects
    """
    index = DatetimeIndex([datetime.datetime.fromtimestamp(v) for v in y['time']])
    y = DataFrame.from_records(y, index=index, exclude=['time'])
    y['adjclose'] = y['close']

    for div in divs:
        if div['split'] + div['purchase'] + div['dividend'] == 0:
            continue
        d = Dividend(div)
        d.adjust(y)

    factor = y['adjclose'] / y['close']
    frame = y.copy()
    frame['open'] = frame['open'] * factor
    frame['high'] = frame['high'] * factor
    frame['low'] = frame['low'] * factor
    frame['close'] = frame['close'] * factor
    frame['volume'] = frame['volume'] * (1 / factor)

    if capitalize:
        columns = [k.capitalize() for k in frame.columns]
        columns[-1] = 'Adjusted'
        frame.columns = columns
        del(frame['Amount'])
    return frame
