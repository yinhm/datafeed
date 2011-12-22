# -*- coding: utf-8 -*-

import sys
import functools
import logging

from datetime import timedelta
from dateutil import parser

from datafeed.bidict import Bidict
from datafeed.exchange import *
from datafeed.quote import *
from datafeed.providers.http_fetcher import *
from datafeed.utils import json_decode

__all__ = ['GoogleSecurity', 'currency2float',
           'GoogleReport', 'GoogleReportFetcher',
           'GoogleDay', 'GoogleDayFetcher',
           'GoogleNewsFetcher']

# See: http://www.google.com/intl/en-US/help/stock_disclaimer.html
# Google finance support more exchanges, adding here if you need it.
_EXCHANGES = Bidict({
        "HK": 'HGK',  #Hongkong
        "SH": "SHA",  #Shanghai
        "SZ": "SHE",  #ShenZhen
        "NASDAQ": "NASDAQ",
        "NYSE": "NYSE",
        "NYSEARCA": "NYSEARCA",
        "AMEX": "AMEX",
        })


def currency2float(currency):
    """convert currency to float

    >>> currency2float("10.08")
    10.08
    >>> currency2float("12,313.66")
    12313.66
    >>> currency2float("102.5M")
    102500000
    """
    if currency == '':
        return ''
    if currency[-1:] == "M":
        currency = currency[:-1]
        return currency2float(currency) * 10**6
    return float(currency.replace(",", ""))


class GoogleSecurity(Security):
    @property
    def _abbr(self):
        """Google finance specific exchange abbr."""
        return _EXCHANGES[str(self.exchange)]

    @classmethod
    def from_string(cls, idstr):
        """Parse a google symbol(eg: NASDAQ:GOOG) string."""
        abbr, symbol = idstr.split(':')
        return cls.from_abbr(abbr, symbol)

    @classmethod
    def from_abbr(cls, abbr, symbol):
        """Create from exchange abbr and symbol."""
        exchange = cls.get_exchange_from_abbr(abbr)
        return cls(exchange, symbol)

    @classmethod
    def get_exchange_from_abbr(cls, abbr):
        """get exchange from google abbr."""
        ex = _EXCHANGES[abbr]
        ex_cls = getattr(sys.modules[__name__], ex)
        return ex_cls()
        

class GoogleReport(Report):

    # This only contains common tags.
    # You could retrieve special tag data from self._raw_data.
    _TAGS_DEFINITION = {
        't': ("symbol", str),
        "e": ("abbr", str),
        'op': ("open", currency2float),
        'hi': ("high", currency2float),
        'lo': ("low", currency2float),
        'lt': ("time", parser.parse),
        'l':  ("price", currency2float),
        'c':  ("change", currency2float),
        'vo': ("volume", currency2float)
        }

    _raw_data = {}
        
    def __init__(self, raw_data):
        self.assert_raw(raw_data)
        self._raw_data = raw_data

        data = {}
        for key, value in self._TAGS_DEFINITION.iteritems():
            data[value[0]] = value[1](raw_data[key])
        security = GoogleSecurity.from_abbr(data.pop('abbr'),
                                            data.pop('symbol'))

        super(GoogleReport, self).__init__(security, data)

    def assert_raw(self, raw_data):
        assert isinstance(raw_data['t'], basestring)
        assert isinstance(raw_data['e'], basestring)
        assert isinstance(raw_data['l'], basestring)
        assert isinstance(raw_data['lt'], basestring)
        assert isinstance(raw_data['vo'], basestring)

    def __getitem__(self, key):
        """Proxy to untouched raw data."""
        return self._raw_data[key]

    @property
    def preclose(self):
        return self.price - self.change

    @staticmethod
    def parse(rawdata):
        # don't known why & escaped.
        data = rawdata.strip()[3:].replace('\\x', '')
        parsed = json_decode(data)
        return (GoogleReport(x) for x in parsed)


class GoogleDay(Day):

    _DEFINITIONS = (
        ("date", lambda x: parser.parse(x).date()),
        ("open", currency2float),
        ("high", currency2float),
        ("low", currency2float),
        ("close", currency2float),
        ("volume", currency2float))

    def __init__(self, security, raw_data):
        assert len(raw_data) == 6

        data = {}
        i = 0
        for conf in self._DEFINITIONS:
            data[conf[0]] = conf[1](raw_data[i])
            i += 1

        super(GoogleDay, self).__init__(security, data)

    @staticmethod
    def parse(security, rawdata):
        import csv
        from cStringIO import StringIO
        
        f = StringIO(rawdata)
        r = csv.reader(f)
        r.next() # skip header
        return (GoogleDay(security, line) for line in r)


class GoogleReportFetcher(Fetcher):

    # Maximum number of stocks we'll batch fetch.
    _MAX_REQUEST_SIZE = 100
    
    def __init__(self, base_url='http://www.google.com/finance/info',
                 time_out=20, max_clients=10, request_size=100):
        assert request_size <= self._MAX_REQUEST_SIZE
        
        super(GoogleReportFetcher, self).__init__(base_url, time_out, max_clients)
        self._request_size = request_size

    def _fetching_urls(self, *args, **kwargs):
        gids = (str(s) for s in args)
        gids = self._slice(gids)

        return (self._make_url(filter(lambda x: x != None, i)) for i in gids)

    def _make_url(self, ids):
        """Make url to fetch.

        example:        
        http://www.google.com/finance/info?q=SHA:000001,NASDAQ:GOOG&infotype=infoquoteall
        """
        return "%s?q=%s&infotype=infoquoteall" % (self._base_url,
                                                  ','.join(ids))

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
        finally:
            self.stop()


class GoogleDayFetcher(DayFetcher):

    def __init__(self, base_url='http://www.google.com/finance/historical',
                 time_out=20, max_clients=10):
        super(GoogleDayFetcher, self).__init__(base_url, time_out, max_clients)

    def _make_url(self, security, **kwargs):
        """Generate url to fetch.

        example:
        
        http://www.google.com/finance/historical?q=NASDAQ:GOOG&startdate=2011-04-01&enddate=2011-04-28&output=csv

        Google finance return one day more data, typically this isn't a
        problem, we decrease the enddate by one day for passing tests.
        """
        url_format = "%s?q=%s&startdate=%s&enddate=%s&output=csv"
        return url_format % (self._base_url,
                             str(security),
                             kwargs['start_date'],
                             kwargs['end_date'] -  timedelta(days=1))


class GoogleNewsFetcher(Fetcher):
    _BASE_URL = "http://www.google.com/finance/company_news?q=%s&output=rss"

    # Maximum number of stocks we'll batch fetch.
    _MAX_REQUEST_SIZE = 100
    
    def __init__(self, base_url=_BASE_URL, time_out=10, max_clients=5):
        super(GoogleNewsFetcher, self).__init__(base_url, time_out, max_clients)

    def _fetching_urls(self, *args, **kwargs):
        return (self._make_url(str(security)) for security in args)

    def _make_url(self, symbol):
        """Make url to fetch.

        example:
        http://www.google.com/finance/company_news?q=NASDAQ:GOOG&output=rss
        """
        return self._base_url % symbol

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
