#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2011 yinhm

import datetime
import numpy as np

from pandas import TimeSeries

    
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
        if self.ex_date <= frame.index[0]: # no adjustment needed
            return True

        self._divide(frame)
        self._split(frame)

    def _divide(self, frame):
        if self.cash_afterward == 0:
            return

        cashes = [self.cash_afterward, 0.0]
        adj_day = self.ex_date - datetime.timedelta(days=1)
        indexes = []
        indexes.append(adj_day)
        indexes.append(datetime.date.today())
        
        cashes = TimeSeries(cashes, index=indexes)
        ri_cashes = cashes.reindex(frame.index, method='backfill')

        frame['adjclose'] = frame['adjclose'] - ri_cashes

    def _split(self, frame):
        if self.share_afterward == 1:
            return

        splits = [self.share_afterward, 1.0]
        adj_day = self.ex_date - datetime.timedelta(days=1)
        indexes = []
        indexes.append(adj_day)
        indexes.append(datetime.date.today())
        
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
