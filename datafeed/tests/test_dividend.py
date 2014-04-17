from __future__ import with_statement

import unittest

import datetime
import numpy as np
import time

from pandas import DataFrame, lib
from pandas import TimeSeries

from datafeed.datastore import Day
from datafeed.dividend import Dividend, adjust


def date2unixtime(date):
    return int(time.mktime(date.timetuple()))


class DividendTest(unittest.TestCase):
    dtype = [('time', '<i4'),
             ('split', '<f4'),
             ('purchase', '<f4'),
             ('purchase_price', '<f4'),
             ('dividend', '<f4')]

    def floatEqual(self, x, y):
        if (x - y) < 0.05:
            return True
        else:
            return False

    def test_adjust_divide_or_split(self):
        #http://help.yahoo.com/kb/index?locale=en_US&page=content&y=PROD_FIN&id=SLN2311

        ohlcs = np.array([
                (date2unixtime(datetime.date(2003, 2, 13)),
                 46.99, 46.99, 46.99, 46.99, 675114.0, 758148608.0),
                (date2unixtime(datetime.date(2003, 2, 14)),
                 48.30, 48.30, 48.30, 48.30, 675114.0, 758148608.0),
                (date2unixtime(datetime.date(2003, 2, 18)),
                 24.96, 24.96, 24.96, 24.96, 675114.0, 758148608.0),
                (date2unixtime(datetime.date(2003, 2, 19)),
                 24.53, 24.53, 24.53, 24.53, 675114.0, 758148608.0),
                ], dtype=Day.DTYPE)

        dividends = np.array([
                (date2unixtime(datetime.date(2003, 2, 18)),
                 1.0, 0.0, 0.0, 0.0), # Split 2:1
                (date2unixtime(datetime.date(2003, 2, 19)),
                 0.0, 0.0, 0.0, 0.08), # 0.08 cash dividend
                ], dtype=self.dtype)


        index = np.array([datetime.datetime.fromtimestamp(v) for v in ohlcs['time']],
                         dtype=object)
        y = DataFrame.from_records(ohlcs, index=index, exclude=['time'])
        y['adjclose'] = y['close']

        for div in dividends:
            d = Dividend(div)
            d.adjust(y)

        adjclose = y.xs(datetime.datetime(2003, 2, 13))['adjclose']
        self.assertTrue(self.floatEqual(adjclose, 23.42))

        adjclose = y.xs(datetime.datetime(2003, 2, 14))['adjclose']
        self.assertTrue(self.floatEqual(adjclose, 24.07))
        
        adjclose = y.xs(datetime.datetime(2003, 2, 18))['adjclose']
        self.assertTrue(self.floatEqual(adjclose, 24.88))
        
        adjclose = y.xs(datetime.datetime(2003, 2, 19))['adjclose']
        self.assertTrue(self.floatEqual(adjclose, 24.53))

    def test_adjust_divide_and_split(self):
        ohlcs = np.array([
            (1277222400, 20.739999771118164, 21.139999389648438, 20.68000030517578,
             20.860000610351562, 506320.0, 1058136640.0),
            (1277308800, 13.5, 13.880000114440918, 13.5,
             13.609999656677246, 372504.0, 509364896.0),
            (1277740800, 12.869999885559082, 12.970000267028809, 12.0,
             12.010000228881836, 785225.0, 971340736.0)
        ], dtype=Day.DTYPE)

        dividends = np.array([
                (1062028800, 0.0, 0.0, 0.0, 0.003700000001117587),
                (1086912000, 0.0, 0.0, 0.0, 0.10999999940395355),
                (1121385600, 0.0, 0.0, 0.0, 0.05000000074505806),
                (1151971200, 0.0, 0.0, 0.0, 0.11999999731779099),
                (1179705600, 0.0, 0.0, 0.0, 0.20000000298023224),
                (1208995200, 1.0, 0.0, 0.0, 0.5),
                (1244678400, 0.0, 0.0, 0.0, 0.5),
                (1277337600, 0.5, 0.0, 0.0, 0.5),
                (1308182400, 0.0, 0.0, 0.0, 0.5)                
                ], dtype=self.dtype)

        index = np.array([datetime.datetime.fromtimestamp(v) for v in ohlcs['time']],
                         dtype=object)
        y = DataFrame.from_records(ohlcs, index=index, exclude=['time'])
        y['adjclose'] = y['close']

        for div in dividends:
            d = Dividend(div)
            d.adjust(y)

        adjclose = y.xs(datetime.datetime(2010, 6, 29))['adjclose']
        self.assertTrue(self.floatEqual(adjclose, 11.51))

        adjclose = y.xs(datetime.datetime(2010, 6, 24))['adjclose']
        self.assertTrue(self.floatEqual(adjclose, 13.11))

        adjclose = y.xs(datetime.datetime(2010, 6, 24))['adjclose']
        self.assertTrue(self.floatEqual(adjclose, 13.07))

    def test_adjust_purchase(self):
        ohlcs = np.array([
                (1216915200, 24.889999389648438, 25.450000762939453,
                 24.709999084472656, 25.0, 486284.0, 1216462208.0)
                ], dtype=Day.DTYPE)

        dividends = np.array([
                (1058313600, 0.0, 0.0, 0.0, 0.11999999731779099),
                (1084233600, 0.20000000298023224, 0.0, 0.0, 0.09200000017881393),
                (1119225600, 0.5, 0.0, 0.0, 0.10999999940395355),
                (1140739200, 0.08589000254869461, 0.0, 0.0, 0.0),
                (1150416000, 0.0, 0.0, 0.0, 0.07999999821186066),
                (1158796800, 0.0, 0.0, 0.0, 0.18000000715255737),
                (1183507200, 0.0, 0.0, 0.0, 0.11999999731779099),
                (1217203200, 0.0, 0.0, 0.0, 0.2800000011920929),
                (1246579200, 0.30000001192092896, 0.0, 0.0, 0.10000000149011612),
                (1268611200, 0.0, 0.12999999523162842, 8.850000381469727, 0.0),
                (1277942400, 0.0, 0.0, 0.0, 0.20999999344348907),
                (1307664000, 0.0, 0.0, 0.0, 0.28999999165534973)                
                ], dtype=self.dtype)

        index = np.array([datetime.datetime.fromtimestamp(v) for v in ohlcs['time']],
                         dtype=object)
        y = DataFrame.from_records(ohlcs, index=index, exclude=['time'])
        y['adjclose'] = y['close']

        for div in dividends:
            d = Dividend(div)
            d.adjust(y)

        adjclose = y.xs(datetime.datetime(2008, 7, 25))['adjclose']
        self.assertTrue(self.floatEqual(adjclose, 17.28))


    def test_adjust_func(self):
        """Fix for pandas 0.8 release which upgrade datetime
        handling.
        """
        ohlcs = np.array([
                (date2unixtime(datetime.date(2003, 2, 13)),
                 46.99, 46.99, 46.99, 46.99, 675114.0, 758148608.0),
                (date2unixtime(datetime.date(2003, 2, 14)),
                 48.30, 48.30, 48.30, 48.30, 675114.0, 758148608.0),
                (date2unixtime(datetime.date(2003, 2, 18)),
                 24.96, 24.96, 24.96, 24.96, 675114.0, 758148608.0),
                (date2unixtime(datetime.date(2003, 2, 19)),
                 24.53, 24.53, 24.53, 24.53, 675114.0, 758148608.0),
                ], dtype=Day.DTYPE)

        dividends = np.array([
                (date2unixtime(datetime.date(2003, 2, 18)),
                 1.0, 0.0, 0.0, 0.0), # Split 2:1
                (date2unixtime(datetime.date(2003, 2, 19)),
                 0.0, 0.0, 0.0, 0.08), # 0.08 cash dividend
                ], dtype=self.dtype)


        frame = adjust(ohlcs, dividends)
        self.assertEqual(frame.index[0].date(), datetime.date(2003, 2, 13))

        expected = ['open', 'high', 'low', 'close',
                    'volume', 'amount', 'adjclose']
        self.assert_(np.array_equal(frame.columns, expected))

        day = frame.ix[datetime.datetime(2003, 2, 13)]
        self.assertTrue(self.floatEqual(day['adjclose'], 23.415))

        expected = ['Open', 'High', 'Low', 'Close',
                    'Volume', 'Adjusted']
        frame = adjust(ohlcs, [], capitalize=True)
        self.assert_(np.array_equal(frame.columns, expected))

    def test_adjust_func_should_not_skipped(self):
        y = np.array([
            (1326643200, 22.50, 22.91, 20.65, 20.71, 4551.0, 9873878.0),
            (1326729600, 21.75, 22.78, 21.40, 22.78, 6053.0, 13547097.0),
            (1326816000, 23.90, 24.77, 22.0, 22.5, 11126.0, 26537980.0),
            (1326902400, 22.5, 23.98, 22.05, 23.55, 5983.0, 13886342.0),
            (1326988800, 23.56, 23.90, 23.35, 23.70, 3832.0, 9089978.0)
          ], dtype=Day.DTYPE)

        dividends = np.array([
            (1369008000, 0.5, 0.0, 0.0, 0.15),
            (1340064000, 1.0, 0.0, 0.0, 0.20)
        ], dtype=self.dtype)

        frame = adjust(y, dividends)

        day = frame.ix[datetime.datetime(2012, 1, 20)]
        self.assertTrue(self.floatEqual(day['close'], 7.75))


if __name__ == '__main__':
    unittest.main()
