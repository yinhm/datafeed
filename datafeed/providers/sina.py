# -*- coding: utf-8 -*-

"""
Stocks:
http://hq.sinajs.cn/list=sh600028,sz000100


Indexes:
http://hq.sinajs.cn/list=s_sh000001


Charts:
http://image.sinajs.cn/newchart/min/n/sh000001.gif
http://image.sinajs.cn/newchart/daily/n/sh000001.gif
"""

import functools
import logging
import sys

from dateutil import parser

from datafeed.exchange import *
from datafeed.quote import *
from datafeed.providers.http_fetcher import Fetcher
from tornado.escape import json_decode

__all__ = ['SinaSecurity', 'SinaReport', 'SinaReportFetcher']

# Sina finance 
_EXCHANGES = {
        "HK": 'HGK',  #Hongkong
        "SH": "SHA",  #Shanghai
        "SZ": "SHE",  #ShenZhen
        "NASDAQ": "NASDAQ", # NASDAQ
        }


class SinaSecurity(Security):
    def __str__(self):
        """Symbol with exchange abbr suffix"""
        return "%s%s" % (self._abbr, self.symbol)

    @property
    def _abbr(self):
        """Sina finance specific exchange abbr."""
        return str(self.exchange).lower()

    @classmethod
    def from_string(cls, idstr):
        abbr = idstr[:2]
        symbol = idstr[2:]
        exchange = cls.get_exchange_from_abbr(abbr)
        return cls(exchange, symbol)

    @classmethod
    def get_exchange_from_abbr(cls, abbr):
        """Get exchange from sina abbr."""
        klass = getattr(sys.modules[__name__], abbr.upper())
        return klass()
        

class SinaReport(Report):

    # Data example:
    # var hq_str_sh600028="中国石化,8.64,8.64,8.68,8.71,8.58,8.68,8.69,
    #   27761321,240634267,11289,8.68,759700,8.67,556338,8.66,455296,8.65,
    #   56600,8.64,143671,8.69,341859,8.70,361255,8.71,314051,8.72,342155,8.73,
    #   2011-05-03,15:03:11";'''
    _DEFINITIONS = (
        ("name", str),
        ("open", float),
        ("preclose", float),
        ("price", float),
        ("high", float),
        ("low", float),
        ("bid", float),
        ("ask", float),
        ("volume", int),
        ("amount", float),
        ("bid1", int),
        ("bidp1", float),
        ("bid2", int),
        ("bidp2", float),
        ("bid3", int),
        ("bidp3", float),
        ("bid4", int),
        ("bidp4", float),
        ("bid5", int),
        ("bidp5", float),
        ("ask1", int),
        ("askp1", float),
        ("ask2", int),
        ("askp2", float),
        ("ask3", int),
        ("askp3", float),
        ("ask4", int),
        ("askp4", float),
        ("ask5", int),
        ("askp5", float),
        ("date", lambda x: parser.parse(x).date()),
        ("time", lambda x: parser.parse(x))
        )
    
    def __init__(self, security, raw_data):
        assert len(raw_data) == 32

        data = {}
        i = 0
        for conf in self._DEFINITIONS:
            key, callback = conf
            data[key] = callback(raw_data[i])
            i += 1
        
        super(SinaReport, self).__init__(security, data)

    @staticmethod
    def parse(rawdata):
        from cStringIO import StringIO
        
        f = StringIO(rawdata)
        return (SinaReport.parse_line(line) for line in f)

    @staticmethod
    def parse_line(line):
        splited = line.split('"')
        idstr = splited[0].split('_').pop()[:-1]
        s = SinaSecurity.from_string(idstr)
        return SinaReport(s, splited[1].split(','))


class SinaReportFetcher(Fetcher):

    # Maximum number of stocks we'll batch fetch.
    _MAX_REQUEST_SIZE = 100
    
    def __init__(self, base_url='http://hq.sinajs.cn',
                 time_out=20, max_clients=10, request_size=100):
        assert request_size <= self._MAX_REQUEST_SIZE
        
        super(SinaReportFetcher, self).__init__(base_url, time_out, max_clients)
        self._request_size = request_size

    def _fetching_urls(self, *args, **kwargs):
        ids = (str(s) for s in args)
        ids = self._slice(ids)

        return (self._make_url(filter(lambda x: x != None, i)) for i in ids)

    def _make_url(self, ids):
        return "%s/list=%s" % (self._base_url, ','.join(ids))

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
