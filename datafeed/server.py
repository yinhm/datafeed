#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2010 yinhm

'''A datafeed server.

All datefeed R/W IO should delegate to this.


Supported commands
==================

    auth
    get_mtime
    get_list
    get_report
    get_reports
    get_minute
    get_1minute
    get_5minute
    get_day
    get_dividend
    get_sector
    get_stats
    get_report
    put_reports
    put_minute
    put_1minute
    put_5minute
    put_day

    
Client Protocol
===============

A redis like protocol which is using plain text and binary safe.

Requests

This is the general form:

    *<number of arguments> CR LF
    $<number of bytes of argument 1> CR LF
    <argument data> CR LF
    ...
    $<number of bytes of argument N> CR LF
    <argument data> CR LF

See the following example:

    *3
    $3
    SET
    $5
    mykey
    $7
    myvalue
    $3
    npy
    
This is how the above command looks as a quoted string, so that it is possible
to see the exact value of every byte in the query:

    "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n$3\r\nnpy\r\n"

The last argument always be format type, format should be one of:

    npy, zip, json

Resoponse:

Datafeed server will reply to commands with different kinds of replies. It is
possible to check the kind of reply from the first byte sent by the server:
    
    With a single line reply the first byte of the reply will be "+"
    With an error message the first byte of the reply will be "-"
    With bulk reply the first byte of the reply will be "$"
    

Notice
======
We do not support redis like multi gets or multi bulk replies.
For more details: http://redis.io/topics/protocol
'''
import datetime
import errno
import logging
import marshal
import os
import re
import sys
import time
import zlib

from cStringIO import StringIO

import numpy as np

from tornado import iostream
from tornado import stack_context
try:
    from tornado.tcpserver import TCPServer # tornado 3.x
except ImportError:
    from tornado.netutil import TCPServer # tornado 2.x

from datafeed import datastore
from datafeed.utils import json_encode


__all__ = ['Server', 'Connection', 'Application', 'Request', 'Handler']

class Server(TCPServer):
    def __init__(self, request_callback, io_loop=None, auth_password=None, **kwargs):
        self.request_callback = request_callback
        self.stats = Stats()
        self.auth_password = auth_password
        self.require_auth = False
        if self.auth_password:
            self.require_auth = True
        TCPServer.__init__(self, io_loop=io_loop, **kwargs)

    def start(self):
        """Start a single process."""
        super(Server, self).start(num_processes=1)

    def handle_stream(self, stream, address):
        Connection(stream, address, self.stats,
                   self.require_auth, self.auth_password, self.request_callback)

    def log_stats(self):
        self.stats.log()


class Stats(dict):
    def record(self, method, time):
        if not self.has_key(method):
            self.__setitem__(method, {'min':time, 'max':time, 'total':0, 'count':0})

        item = self.__getitem__(method)
        if time < item['min']:
            item['min'] = time
        if time > item['max']:
            item['max'] = time
        item['total'] += time
        item['count'] += 1

    def log(self):
        msg = ["\nmethod\tmin\tmax\ttotal\tcount"]
        for method, item in self.iteritems():
            msg.append("%s\t\t%.2f\t%.2f\t%.2f\t%d" % \
                           (method, item['min'], item['max'], item['total'], item['count']))
        logging.info("\n".join(msg))


class Connection(object):

    def __init__(self, stream, address, stats, require_auth, auth_password, request_callback=None):
        self.stream = stream
        self.address = address
        self.stats = stats
        self.require_auth = require_auth
        self.auth_password = auth_password
        self.authenticated = False
        self.request_callback = request_callback
        self._request = Request(connection=self)
        self._request_finished = False

        # Save stack context here, outside of any request.  This keeps
        # contexts from one request from leaking into the next.
        self._on_request_wrap = stack_context.wrap(self._on_request)
        self.stream.read_until('\r\n', self._on_request_wrap)

    def write(self, chunk):
        assert self._request, "Request closed"
        if not self.stream.closed():
            self.stream.write(chunk, self._on_write_complete)

    def finish(self):
        assert self._request, "Request closed"
        self._request_finished = True
        if not self.stream.writing():
            self._finish_request()

    def disconnect(self):
        self.stream.close()

    def auth(self, password):
        '''Verify password and set authenticated if match.'''
        if not self.require_auth:
            return True

        if password == self.auth_password:
            self.authenticated = True
        else:
            self.authenticated = False
        return self.authenticated

    def _on_write_complete(self):
        if self._request_finished:
            self._finish_request()

    def _finish_request(self):
        self._request = None
        self._request_finished = False
        self.stream.read_until("\r\n", self._on_request_wrap)

    def _on_request(self, data):
        self._request = Request(connection=self)

        request_type = data[0]
        if request_type != '*':
            if data.strip() == 'quit':
                return self.disconnect()
            else:
                return self._on_request_error(data)
            
        # *<number of arguments> CR LF
        try:
            self._args_count = int(data[1:-2])
            self.stream.read_until("\r\n", self._on_argument_head)
        except ValueError:
            return self._on_request_error(data)

    def _on_request_error(self, data=None):
        self.write("-ERR unknown command %s\r\n" % data)

    def _on_argument_head(self, data):
        request_type = data[0]
        if request_type != '$':
            return self._on_request_error()

        # $<number of bytes of argument N> CR LF
        # <argument data> CR LF
        bytes = int(data[1:-2])
        self.stream.read_bytes(bytes + 2, self._on_argument_data)
        
    def _on_argument_data(self, data):
        self._request.args.append(data[:-2])
        self._args_count = self._args_count - 1

        if self._args_count > 0:
            self.stream.read_until("\r\n", self._on_argument_head)
        else:
            self.request_callback(self._request)
        

class Request(object):
    def __init__(self, connection, *args):
        self.connection = connection
        self._start_time = time.time()
        self._finish_time = None
        self.args = list(args)

        self.response_message = ""

    @property
    def method(self):
        return self.args[0].lower()

    def write(self, chunk):
        """Writes the given chunk to the response stream."""
        assert isinstance(chunk, str)
        if self.connection:
            self.connection.write(chunk)

    def write_ok(self):
        """Shortcut of write OK."""
        self.write("+OK\r\n")

    def write_error(self, msg=''):
        """Shortcut of write OK."""
        self.write("-ERR %s\r\n" % msg)

    def finish(self):
        """Finishes this HTTP request on the open connection."""
        if self.connection:
            self.connection.finish()
        self._finish_time = time.time()

    def record_stats(self):
        if self.connection:
            self.connection.stats.record(self.method, self.request_time())

    def request_time(self):
        """Returns the amount of time it took for this request to execute."""
        if self._finish_time is None:
            return time.time() - self._start_time
        else:
            return self._finish_time - self._start_time


class Application(object):

    def __init__(self, datadir, exchange, **kwargs):
        self.dbm = datastore.Manager(datadir, exchange)
        self.exchange = exchange

        if 'handler' in kwargs:
            self._handler = kwargs['handler']
        else:
            self._handler = Handler

    def __call__(self, request):
        handler = self._handler(self, request)
        handler._execute()
        return handler


class Handler(object):

    SUPPORTED_METHODS = ('auth',
                         'get_last_quote_time',
                         'get_mtime',
                         'get_list',
                         'get_report',
                         'get_reports',
                         'get_minute',
                         'get_1minute',
                         'get_5minute',
                         'get_day',
                         'get_dividend',
                         'get_sector',
                         'get_stats',
                         'get_report',
                         'put_reports',
                         'put_minute',
                         'put_1minute',
                         'put_5minute',
                         'put_day')

    def __init__(self, application, request, **kwargs):
        self.application = application
        self.request = request
        self.dbm = application.dbm

        self._finished = False
    
    def auth(self, password, format='plain'):
        """Authticate.
        """
        ret = self.request.connection.auth(password)
        if ret:
            self.request.write_ok()
        else:
            self.request.write_error("invalid password")
            if not self._finished:
                self.finish()

    def get_mtime(self, *args, **kwds):
        """Return last quote timestamp.
        """
        self.request.write(":%d\r\n" % self.dbm.mtime)

    def get_last_quote_time(self, *args, **kwds):
        """Return last quote timestamp.
        """
        logging.warning("Deprecated, using get_mtime instead.")
        self.get_mtime()

    def get_list(self, match=None, format='json'):
        assert format == 'json'

        if match != '':
            _re = re.compile('^(%s)' % match, re.I)
            ret = dict([(r, v) for r, v in self.dbm.reportstore.iteritems() \
                            if _re.search(r)])
        else:
            ret = self.dbm.reportstore.to_dict()

        return self._write_response(json_encode(ret))

    def get_report(self, symbol, format):
        try:
            data = self.dbm.get_report(symbol)
            if format == 'json':
                data = json_encode(data)
            self._write_response(data)
        except KeyError:
            self.request.write("-ERR Symbol %s not exists.\r\n" % symbol)

    def get_reports(self, *args):
        assert len(args) > 1
        format = args[-1]
        data = self.dbm.get_reports(*args[:-1])

        if format == 'json':
            data = json_encode(data)
            
        self._write_response(data)

    def get_minute(self, symbol, timestamp, format='npy'):
        """Get daily minutes history.

        Arguments:
        symbol: String of security.
        timestamp: Which day data to get.
        format: npy or json
        """
        try:
            ts = int(timestamp)
            if ts > 0:
                store = self.dbm.get_minutestore_at(ts)
            else:
                store = self.dbm.minutestore
                
            y = store.get(symbol)

            if format == 'npy':
                memfile = StringIO()
                np.save(memfile, y)
                data = memfile.getvalue()
                del(y)
            else:
                data = json_encode(y.tolist())
            self._write_response(data)
        except KeyError:
            self.request.write("-ERR Symbol %s not exists.\r\n" % symbol)

    def get_1minute(self, symbol, date, format='npy'):
        """Get 5min historical quotes.

        Arguments:
          symbol: String of security.
          date: Which day data to get.
          format: npy or json
        """
        try:
            if isinstance(date, str):
                date = datetime.datetime.strptime(date, '%Y%m%d').date()

            y = self.dbm.oneminstore.get(symbol, date)

            if format == 'npy':
                memfile = StringIO()
                np.save(memfile, y)
                data = memfile.getvalue()
                del(y)
            else:
                data = json_encode(y.tolist())
            self._write_response(data)
        except KeyError:
            self.request.write("-ERR No data.\r\n")

    def get_5minute(self, symbol, date, format='npy'):
        """Get 5min historical quotes.

        Arguments:
          symbol: String of security.
          date: Which day data to get.
          format: npy or json
        """
        try:
            if isinstance(date, str):
                date = datetime.datetime.strptime(date, '%Y%m%d').date()

            y = self.dbm.fiveminstore.get(symbol, date)

            if format == 'npy':
                memfile = StringIO()
                np.save(memfile, y)
                data = memfile.getvalue()
                del(y)
            else:
                data = json_encode(y.tolist())
            self._write_response(data)
        except KeyError:
            self.request.write("-ERR No data.\r\n")

    def get_dividend(self, symbol, format='npy'):
        try:
            try:
                y = self.dbm.divstore.get(symbol)[:]
            except TypeError:
                y = np.zeros(0)

            if format == 'npy':
                memfile = StringIO()
                np.save(memfile, y)
                data = memfile.getvalue()
                del(y)
            else:
                data = json_encode(y.tolist())
            self._write_response(data)
        except KeyError:
            self.request.write("-ERR Symbol %s not exists.\r\n" % symbol)

    def get_sector(self, name, format='json'):
        try:
            data = self.dbm.sectorstore[name]
            if format == 'json':
                data = json_encode(data)
            self._write_response(data)
        except KeyError:
            self.request.write("-ERR Sector %s not exists.\r\n" % name)

    def get_stats(self, name, format='json'):
        stats = self.request.connection.stats
        self._write_response(json_encode(stats))

    def get_day(self, symbol, length_or_date, format='npy'):
        """Get OHLCs quotes.

        Return chronicle ordered quotes.
        """
        try:
            if len(length_or_date) == 8: # eg: 20101209
                date = datetime.datetime.strptime(length_or_date, '%Y%m%d').date()
                y = self.dbm.daystore.get_by_date(symbol, date)
            else:
                length = length_or_date
                y = self.dbm.daystore.get(symbol, int(length))
                if length == 1:
                    y = y[0]

            if format == 'npy':
                memfile = StringIO()
                np.save(memfile, y)
                data = memfile.getvalue()
                del(y)
            else:
                data = json_encode(y.tolist())
            self._write_response(data)
        except KeyError:
            self.request.write("-ERR Symbol %s not exists.\r\n" % symbol)

    def _write_response(self, ret):
        self.request.write("$%s\r\n%s\r\n" % (len(ret), ret))
        
    def put_reports(self, data, format='zip'):
        """Update reports from data.

        Data Format:

        data should be zlib compressed python dicts serializing by marshal.
        """
        assert format == 'zip'

        try:
            data = marshal.loads(zlib.decompress(data))
            assert isinstance(data, dict)
        except StandardError:
            return self.request.write("-ERR wrong data format\r\n")
        self.dbm.update_reports(data)
        self.request.write_ok()
        
    def put_minute(self, symbol, data, format='npy'):
        func = getattr(self.dbm, "update_minute")
        self._put(func, symbol, data, format)
        
    def put_1minute(self, symbol, data, format='npy'):
        self.dbm.oneminstore.update(symbol, np.load(StringIO(data)))
        self.request.write_ok()

    def put_5minute(self, symbol, data, format='npy'):
        self.dbm.fiveminstore.update(symbol, np.load(StringIO(data)))
        self.request.write_ok()

    def put_day(self, symbol, data, format='npy'):
        func = getattr(self.dbm, "update_day")
        self._put(func, symbol, data, format)
        
    def _put(self, func, symbol, data, format):
        assert format == 'npy'
        
        start_time = time.time()

        try:
            data = np.load(StringIO(data))
        except StandardError:
            return self.request.write("-ERR wrong data format\r\n")
        
        end_time = time.time()
        parse_time = 1000.0 * (end_time - start_time)
        logging.info("proto parse: %.2fms", parse_time)
        
        if data != None:
            func(symbol, data)

        self.request.write("+OK\r\n")

    def finish(self):
        """Finishes this response, ending the HTTP request."""
        assert not self._finished
        self.request.finish()
        self._log()
        self._finished = True

    def _execute(self):
        conn = self.request.connection
        # Check if the user is authenticated
        if conn and conn.require_auth and not \
                conn.authenticated and \
                self.request.method != 'auth':
            self.request.write("-ERR operation not permitted\r\n")
            if not self._finished:
                self.finish()

        if self.request.method not in self.SUPPORTED_METHODS:
            logging.error("Unknown command.\r\n")
            self.request.write("-ERR UNKNOWN COMMAND\r\n")
            if not self._finished:
                self.finish()

        if not self._finished:
            arguments = self.request.args[1:] 
            getattr(self, self.request.method)(*arguments)
            if not self._finished:
                self.finish()

    def _log(self):
        self.request.record_stats()
        request_time = 1000.0 * self.request.request_time()
        logging.info("%s %.2fms", self._request_summary(), request_time)

    def _request_summary(self):
        return "%s %s" % (self.request.method, self.request.response_message)
