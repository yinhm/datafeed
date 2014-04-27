import errno
import functools
import json
import marshal
import socket
import time
import zlib

import numpy as np

from cStringIO import StringIO

from datafeed.utils import json_decode


__all__ = ['Client', 'ConnectionError']


class ConnectionError(Exception):
    pass


def put_zipped(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwds):
        assert len(args) == 2 or len(args) == 3

        args = list(args)
        # timstamp
        if len(args) == 2:
            args.insert(1, time.time())
        args[1] = str(args[1])

        # rawdata
        rawdata = args[-1]
        if ('jsondata' not in kwds) or \
           kwds['jsondata'] != True:
            rawdata = json.dumps(rawdata)
        args[-1] = zlib.compress(rawdata)
        args.append('zip')
        apiname = method.__name__.upper()

        # args should be [symbol, timestamp, rawdata, zip]
        return self.execute_command(apiname, *args)
    return wrapper


class Client(object):
    """Manages Tcp communication to and from a datafeed server.
    """

    def __init__(self, host='localhost', port=8082,
                 password=None, socket_timeout=None):
        self._host = host
        self._port = port
        self._password = password
        self._socket_timeout = socket_timeout

        self._sock = None
        self._fp = None

    def connect(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self._host, self._port))
        except socket.error, e:
            # args for socket.error can either be (errno, "message")
            # or just "message"
            if len(e.args) == 1:
                error_message = "Error connecting to %s:%s. %s." % \
                    (self.host, self.port, e.args[0])
            else:
                error_message = "Error %s connecting %s:%s. %s." % \
                    (e.args[0], self._host, self._port, e.args[1])
            raise ConnectionError(error_message)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(self._socket_timeout)
        self._sock = sock
        self._fp = sock.makefile('rb')

        if self._password:
            self.auth()

    @property
    def connected(self):
        return self._sock

    def close(self):
        self.disconnect()

    def disconnect(self):
        if not self.connected:
            return
        try:
            self._sock.close()
        except socket.error:
            pass
        self._sock = None
        self._fp = None

    def reconnect(self):
        self.disconnect()
        self.connect()

    def ensure_connected(self):
        '''TODO: move to a closure?'''
        if not self.connected:
            self.connect()

    def read(self, length=None):
        self.ensure_connected()
        try:
            if length is not None:
                return self._fp.read(length)
            return self._fp.readline()
        except socket.error, e:
            self.disconnect()
            if e.args and e.args[0] == errno.EAGAIN:
                raise StandardError("Error while reading from socket: %s" % \
                                        e.args[1])
        return ''


    #### COMMAND EXECUTION AND PROTOCOL PARSING ####
    def execute_command(self, *args):
        """Sends the command to the server and returns it's response.

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
        """
        return self._execute_command(args[0], args[-1], self._build_data(*args))

    def _build_data(self, *args):
        cmds = ('$%s\r\n%s\r\n' % (len(arg), arg) for arg in args)
        return '*%s\r\n%s' % (len(args), ''.join(cmds))

    def _execute_command(self, command, format, data):
        self.send(data)
        return self._parse_response(command, format)
    
    def send(self, data):
        self.ensure_connected()
        try:
            self._sock.sendall(data)
        except socket.error, e:
            if self.reconnect():
                self._sock.sendall(data)
            else:
                raise StandardError("Error %s while writing to socket. %s." % \
                                        e.args)

    def _parse_response(self, command, format):
        response = self.read()[:-2]  # strip last two characters (\r\n)
        if not response:
            self.disconnect()
            raise StandardError("Socket closed on remote end")

        # server returned a null value
        if response in ('$-1', '*-1'):
            return None
        reply_type, response = response[0], response[1:]

        # server returned an error
        if reply_type == '-':
            if response.startswith('ERR '):
                response = response[4:]
            raise Exception(response)
        # single value
        elif reply_type == '+':
            return response
        # integer value
        elif reply_type == ':':
            return int(response)
        # bulk response
        elif reply_type == '$':
            length = int(response)
            response = length and self.read(length) or ''
            self.read(2) # read the \r\n delimiter

            if format == 'json':
                return json_decode(response)
            elif format == 'npy':
                qdata = StringIO(response)
                return np.load(qdata)
            else:
                return response

        raise Exception("Unknown response type for: %s" % command)

    def auth(self):
        self.execute_command('AUTH', self._password, 'plain')

    def get_mtime(self):
        return self.execute_command('GET_MTIME', 'plain')

    def get_list(self, match='', format='json'):
        return self.execute_command('GET_LIST', match, format)

    def get_meta(self, key, format='json'):
        return self.execute_command('GET_META', key, format)

    def get_tick(self, symbol, format='json'):
        return self.execute_command('GET_TICK', symbol, format)

    def get_report(self, *args, **kwds):
        return self.get_tick(*args, **kwds)

    def get_ticks(self, *args, **kwargs):
        format = 'json'
        if 'format' in kwargs:
            format = kwargs['format']
        args = args + (format,)
        return self.execute_command('GET_TICKS', *args)

    def get_reports(self, *args, **kwds):
        return self.get_ticks(*args, **kwds)

    def get_minute(self, symbol, timestamp=0, format='npy'):
        """Get minute history data.

        timestamp: 0 for last day data.
        """
        assert isinstance(timestamp, int)
        return self.execute_command('GET_MINUTE', symbol, str(timestamp), format)

    def get_1minute(self, symbol, date, format='npy'):
        """Get minute history data.

        date: specific day to retrieve.
        """
        return self.execute_command('GET_1MINUTE', symbol, date, format)

    def get_5minute(self, symbol, date, format='npy'):
        """Get minute history data.

        date: specific day to retrieve.
        """
        return self.execute_command('GET_5MINUTE', symbol, date, format)

    def get_day(self, symbol, length_or_date, format='npy'):
        assert isinstance(length_or_date, int) or len(length_or_date) == 8
        return self.execute_command('GET_DAY', symbol, str(length_or_date), format)

    def get_dividend(self, symbol, format='npy'):
        return self.execute_command('GET_DIVIDEND', symbol, format)

    def get_fin(self, symbol, format='npy'):
        return self.execute_command('GET_FIN', symbol, format)

    def get_sector(self, name, format='json'):
        return self.execute_command('GET_SECTOR', name, format)

    def get_stats(self):
        return self.execute_command('GET_STATS', 'json')

    def put_ticks(self, adict):
        assert isinstance(adict, dict)
        data = zlib.compress(marshal.dumps(adict))
        return self.execute_command('PUT_TICKS', data, 'zip')

    @put_zipped
    def put_meta(self, key, value):
        pass

    @put_zipped
    def put_tick(self, symbol, timestamp, adict):
        pass

    @put_zipped
    def put_depth(self, symbol, timestamp, rawdata):
        pass

    @put_zipped
    def put_trade(self, symbol, timestamp, rawdata):
        pass

    @put_zipped
    def mput_trade(self, symbol, rawdata):
        pass

    def put_reports(self, *args, **kwds):
        return self.put_ticks(*args, **kwds)

    def put_minute(self, symbol, rawdata):
        memfile = StringIO()
        np.save(memfile, rawdata)
        return self.execute_command('PUT_MINUTE', symbol, memfile.getvalue(), 'npy')

    def put_1minute(self, symbol, rawdata):
        memfile = StringIO()
        np.save(memfile, rawdata)
        return self.execute_command('PUT_1MINUTE', symbol, memfile.getvalue(), 'npy')

    def put_5minute(self, symbol, rawdata):
        memfile = StringIO()
        np.save(memfile, rawdata)
        return self.execute_command('PUT_5MINUTE', symbol, memfile.getvalue(), 'npy')

    def put_day(self, symbol, rawdata):
        memfile = StringIO()
        np.save(memfile, rawdata)
        return self.execute_command('PUT_DAY', symbol, memfile.getvalue(), 'npy')

    def archive_minute(self):
        return self.execute_command('ARCHIVE_MINUTE')
