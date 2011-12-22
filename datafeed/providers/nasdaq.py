# -*- coding: utf-8 -*-

"""
NASDAQ && NYSE stocks list.
http://www.nasdaq.com/screening/companies-by-industry.aspx
"""
import csv
import functools
import logging
import sys

from cStringIO import StringIO

from tornado import httpclient
from tornado import ioloop

from datafeed.exchange import *
from datafeed.quote import *
from datafeed.providers.http_fetcher import *
from datafeed.utils import json_decode

__all__ = ['NasdaqSecurity',
           'NasdaqList', 'NasdaqListFetcher']


class NasdaqSecurity(Security):
    pass

class NasdaqList(SecurityList):

    # Tags format defined by NasdaqReportFetcher which is:
    # "sl1d1t1c1ohgv"
    # FIXME: Nasdaq quotes became N/A during session after hours.
    _DEFINITIONS = (
        ("symbol", lambda x: x.strip()),
        ("name", str),
        ("price", float),
        ("market_cap", str),
        ("ipo_year", str),
        ("sector", str),
        ("industry", str),
        ("summary", str)
        )

    def __init__(self, exchange, raw_data):
        raw_data.pop()
        assert len(raw_data) == len(self._DEFINITIONS)

        i = 0
        data = {}
        for conf in self._DEFINITIONS:
            key, callback = conf
            data[key] = callback(raw_data[i])
            i += 1

        security = NasdaqSecurity(exchange, data.pop('symbol'), data['name'])
        super(NasdaqList, self).__init__(security, data)

    def __repr__(self):
        return "%s\r\n%s" % (self.security, self.name)

    def __str__(self):
        return "%s" % (self.security, )

    @staticmethod
    def parse(exchange, rawdata):
        """Parse security list for specific exchange.
        """
        f = StringIO(rawdata)
        r = csv.reader(f)
        r.next()
        return (NasdaqList(exchange, line) for line in r)


class NasdaqListFetcher(Fetcher):

    # Maximum number of stocks we'll batch fetch.
    _MAX_REQUEST_SIZE = 100
    _BASE_URL = "http://www.nasdaq.com/screening/companies-by-industry.aspx"
    
    def __init__(self, base_url=_BASE_URL,
                 time_out=20, max_clients=10, request_size=100):
        assert request_size <= self._MAX_REQUEST_SIZE
        
        super(NasdaqListFetcher, self).__init__(base_url, time_out, max_clients)
        self._request_size = request_size

    def _fetching_urls(self, *args, **kwargs):
        """Making list of fetching urls from exchanges.
        """
        for arg in args:
            assert isinstance(arg, NYSE) \
                or isinstance(arg, NASDAQ) \
                or isinstance(arg, AMEX)
        return (self._make_url(arg) for arg in args)

    def _make_url(self, exchange):
        """Make url to fetch.

        example:
        http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NYSE&render=download
        """
        return "%s?exchange=%s&render=download" % (self._base_url, exchange)

    def _callback(self, security, **kwargs):
        if 'callback' in kwargs:
            callback = kwargs['callback']
        else:
            callback = None
        return functools.partial(self._handle_request,
                                 callback,
                                 security)

    def _handle_request(self, callback, exchange, response):
        try:
            self.queue_len = self.queue_len - 1

            if response.error:
                logging.error(response.error)
            else:
                callback(exchange, response.body)
        except StandardError:
            logging.error("Wrong data format.")
        finally:
            self.stop()
