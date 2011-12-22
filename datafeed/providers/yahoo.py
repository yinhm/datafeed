# -*- coding: utf-8 -*-

"""
Yahoo Finance API tags:
http://www.gummy-stuff.org/Yahoo-data.htm

See also:
https://github.com/yql/yql-tables/blob/master/yahoo/finance/

Yahoo! Finance news headlines:
http://developer.yahoo.com/finance/
"""
import csv
import functools
import logging
import sys

from cStringIO import StringIO

from dateutil import parser
from tornado import httpclient
from tornado import ioloop

from datafeed.bidict import Bidict
from datafeed.exchange import *
from datafeed.quote import *
from datafeed.providers.http_fetcher import *
from datafeed.utils import json_decode

__all__ = ['YahooSecurity',
           'YahooReport', 'YahooReportFetcher',
           'YahooDay', 'YahooDayFetcher',
           'YahooNewsFetcher']


# See full list of exchangs on Yahoo! finance:
# http://finance.yahoo.com/exchanges
_EXCHANGES = Bidict({
        "HK": "HK",
        "LON": "L",
        "SH": "SS",
        "SZ": "SZ",
        })


class YahooSecurity(Security):
    SUFFIX_NA = (NASDAQ(), NYSE(), AMEX())

    def __init__(self, exchange, *args, **kwds):
        if exchange in (self.SUFFIX_NA):
            exchange = YahooNA()
        super(YahooSecurity, self).__init__(exchange, *args, **kwds)

    def __str__(self):
        """symbol with exchange abbr suffix"""
        if self.exchange == YahooNA():
            ret = self.symbol
        else:
            ret = "%s.%s" % (self.symbol, self._abbr)
        return ret

    @property
    def _abbr(self):
        """Yahoo finance specific exchange abbr."""
        return _EXCHANGES[str(self.exchange)]

    @classmethod
    def from_string(cls, idstr):
        if idstr.find('.') > 0:
            symbol, abbr = idstr.split('.')
            exchange = cls.get_exchange_from_abbr(abbr)
        else:
            symbol = idstr
            # US, Japan, Lodon exchnages on Yahoo! finance have no suffix
            exchange = YahooNA()
        return cls(exchange, symbol)

    @classmethod
    def get_exchange_from_abbr(cls, abbr):
        """get exchange from yahoo abbr"""
        ex = _EXCHANGES[abbr]
        ex_cls = getattr(sys.modules[__name__], ex)
        return ex_cls()
        

class YahooReport(Report):

    # Tags format defined by YahooReportFetcher which is:
    # "sl1d1t1c1ohgv"
    # FIXME: Yahoo quotes became N/A during session after hours.
    _DEFINITIONS = (
        ("symbol", str),
        ("price", float),
        ("date", lambda x: parser.parse(x).date()),
        ("time", parser.parse),
        ("change", float),
        ("open", float),
        ("high", float),
        ("low", float),
        ("volume", float),
        )

    def __init__(self, raw_data):
        assert len(raw_data) == len(self._DEFINITIONS)

        i = 0
        data = {}
        for conf in self._DEFINITIONS:
            key, callback = conf
            data[key] = callback(raw_data[i])
            i += 1

        security = YahooSecurity.from_string(data.pop('symbol'))
        super(YahooReport, self).__init__(security, data)

    @staticmethod
    def parse(rawdata):        
        f = StringIO(rawdata)
        r = csv.reader(f)
        return (YahooReport(line) for line in r)


class YahooDay(Day):

    _DEFINITIONS = (
        ("date", lambda x: parser.parse(x).date()),
        ("open", float),
        ("high", float),
        ("low", float),
        ("close", float),
        ("volume", float),
        ("adjclose", float))

    def __init__(self, security, raw_data):
        assert len(raw_data) == len(self._DEFINITIONS)

        data = {}
        i = 0
        for conf in self._DEFINITIONS:
            data[conf[0]] = conf[1](raw_data[i])
            i += 1

        super(YahooDay, self).__init__(security, data)

    @staticmethod
    def parse(security, rawdata):
        f = StringIO(rawdata)
        r = csv.reader(f)
        r.next() # skip header
        return (YahooDay(security, line) for line in r)


class YahooReportFetcher(Fetcher):

    # Live quotes tags format,
    # consistent with downloads link on the web page.
    _FORMAT = "sl1d1t1c1ohgv"
    
    # Maximum number of stocks we'll batch fetch.
    _MAX_REQUEST_SIZE = 100
    
    def __init__(self, base_url='http://download.finance.yahoo.com/d/quotes.csv',
                 time_out=20, max_clients=10, request_size=100):
        assert request_size <= self._MAX_REQUEST_SIZE
        
        super(YahooReportFetcher, self).__init__(base_url, time_out, max_clients)
        self._request_size = request_size

    def _fetching_urls(self, *args, **kwargs):
        ids = (str(s) for s in args)
        ids = self._slice(ids)

        return (self._make_url(filter(lambda x: x != None, i)) for i in ids)

    def _make_url(self, ids):
        """Make url to fetch.

        example:
        http://download.finance.yahoo.com/d/quotes.csv?s=GOOG+AAPL+600028.SS&f=sl1d1t1c1ohgv&e=.csv
        """
        return "%s?s=%s&f=%s&e=.csv" % (self._base_url, '+'.join(ids), self._FORMAT)

    def _callback(self, security, **kwargs):
        if 'callback' in kwargs:
            callback = kwargs['callback']
        else:
            callback = None
        return functools.partial(self._handle_request, callback)

    def _handle_request(self, callback, response):
        try:
            self.queue_len = self.queue_len - 1

            if response.error:
                logging.error(response.error)
            else:
                callback(response.body)
        except StandardError:
            logging.error("Wrong data format.")
        finally:
            self.stop()


class YahooDayFetcher(DayFetcher):

    def __init__(self, base_url='http://ichart.finance.yahoo.com/table.csv',
                 time_out=20, max_clients=10):
        super(YahooDayFetcher, self).__init__(base_url, time_out, max_clients)

    def _make_url(self, security, **kwargs):
        """Make url to fetch.

        Parameters:
        s  Stock Ticker (for example, MSFT)  
        a  Start Month (0-based; 0=January, 11=December)  
        b  Start Day  
        c  Start Year  
        d  End Month (0-based; 0=January, 11=December)  
        e  End Day  
        f  End Year  
        g  Always use the letter d  

        example:
        http://ichart.finance.yahoo.com/table.csv?s=GOOG&d=4&e=4&f=2011&g=d&a=7&b=19&c=2004&ignore=.csv
        """
        url_format = "%s?s=%s&g=d&a=%s&b=%s&c=%s"
        url_format += "&d=%s&e=%s&f=%s"

        start_date = kwargs['start_date']
        end_date = kwargs['end_date']
        url = url_format % (self._base_url, str(security),
                            start_date.month - 1, start_date.day, start_date.year,
                            end_date.month - 1, end_date.day, end_date.year)
        return url


class YahooNewsFetcher(Fetcher):
    _BASE_URL = "http://feeds.finance.yahoo.com/rss/2.0/headline"
    
    # Maximum number of stocks we'll batch fetch.
    _MAX_REQUEST_SIZE = 100
    
    def __init__(self, base_url=_BASE_URL, time_out=10, max_clients=5):
        super(YahooNewsFetcher, self).__init__(base_url, time_out, max_clients)

    def _fetching_urls(self, *args, **kwargs):
        return (self._make_url(str(security)) for security in args)

    def _make_url(self, symbol):
        """Make url to fetch.

        example:
        http://feeds.finance.yahoo.com/rss/2.0/headline?s=yhoo&region=US&lang=en-US
        """
        return "%s?s=%s&region=US&lang=en-US" % (self._base_url, symbol)

    def _callback(self, security, **kwargs):
        if 'callback' in kwargs:
            callback = kwargs['callback']
        else:
            callback = None
        return functools.partial(self._handle_request,
                                 callback,
                                 security)

    def _handle_request(self, callback, security, response):
        try:
            self.queue_len = self.queue_len - 1

            if response.error:
                logging.error(response.error)
            else:
                callback(security, response)
        finally:
            self.stop()
