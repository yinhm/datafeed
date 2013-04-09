# -*- coding: utf-8 -*-

import functools
import logging
import sys

from tornado.curl_httpclient import CurlAsyncHTTPClient as AsyncHTTPClient 
from tornado import ioloop

 
try:
    from itertools import izip_longest
except ImportError:
    """Python 2.5 support"""
    from itertools import izip, chain, repeat
    if sys.version_info[:2] < (2,6):
        def izip_longest(*args, **kwds):
            # izip_longest('ABCD', 'xy', fillvalue='-') --> Ax By C- D-
            fillvalue = kwds.get('fillvalue')
            def sentinel(counter = ([fillvalue]*(len(args)-1)).pop):
                yield counter()         # yields the fillvalue, or raises IndexError
            fillers = repeat(fillvalue)
            iters = [chain(it, sentinel(), fillers) for it in args]
            try:
                for tup in izip(*iters):
                    yield tup
            except IndexError:
                pass


__all__ = ['Fetcher', 'DayFetcher', 'zip_slice']


class Fetcher(object):
    _MAX_CLIENTS = 10

    def __init__(self, base_url=None, time_out=20, max_clients=10):
        assert isinstance(base_url, basestring)
        assert isinstance(time_out, int)
        assert isinstance(max_clients, int)
        assert max_clients <= self._MAX_CLIENTS
        
        self._base_url = base_url
        self._time_out = time_out
        self._max_clients = max_clients

        self._io_loop = ioloop.IOLoop()

        self.queue_len = 0

    def fetch(self, *args, **kwargs):
        ret = []
        if not len(args) > 0:
            return ret
        
        urls = self._fetching_urls(*args, **kwargs)

        http = AsyncHTTPClient(self._io_loop)
        i = 0
        for url in urls:
            callback = self._callback(args[i], **kwargs)
            logging.info("start urlfetch %s" % url)
            http.fetch(url, callback)
            self.queue_len = self.queue_len + 1
            i += 1

        self._io_loop.start()
        return ret

    def _fetching_urls(self, *args, **kwargs):
        raise NotImplementedError()

    def _slice(self, iterable, fillvalue=None):
        return zip_slice(self._request_size, iterable, fillvalue)

    def _callback(self, security, **kwargs):
        pass

    def stop(self):
        if self.queue_len == 0:
            self._io_loop.stop()


class DayFetcher(Fetcher):

    def _fetching_urls(self, *args, **kwargs):
        assert 'start_date' in kwargs
        assert 'end_date' in kwargs
        
        urls = (self._make_url(s, **kwargs) for s in args)
        return urls

    def _make_url(self, security, **kwargs):
        raise NotImplementedError()

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
                callback(security, response.body)
        except StandardError:
            logging.error("Wrong data format.")
        finally:
            self.stop()


def zip_slice(len_each, iterable, fillvalue=None):
    "zip_slice(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    assert isinstance(len_each, int)
    args = [iter(iterable)] * len_each
    return izip_longest(fillvalue=fillvalue, *args)
