# -*- coding: utf-8 -*-
'''Basic market infos.

#TBD: extract currency from major exchanges.
'''
import sys
import time

from datetime import datetime


__all__ = ['StockExchange',
           'AMEX', 'LON', 'NASDAQ',
           'NYSE', 'HK', 'SH', 'SZ', 'TYO',
           'YahooNA', 'Security']


class StockExchange(object):
    '''Major stock exchanges, see:
    - http://en.wikipedia.org/wiki/Stock_exchange
    - http://www.wikinvest.com/wiki/List_of_Stock_Exchanges
    '''
    _pre_market_session = None
    _market_session = None
    _market_break_session = None
    
    _instances = dict()
    
    def __new__(cls, *args, **kwargs):
        klass = cls.__name__
        if not cls._instances.has_key(klass):
            cls._instances[klass] = super(StockExchange, cls).__new__(
                cls, *args, **kwargs)
        return cls._instances[klass]
    
    @classmethod
    def change_time(cls, hour, minute, day=None, now=None):
        if now:
            day = datetime.fromtimestamp(now)
        if not day:
            day = datetime.today()
        t = time.mktime((day.year, day.month, day.day,
                         hour, minute, 0, 0, 0, 0))

        return t
    
    @classmethod
    def pre_open_time(cls, **kwargs):
        return cls.change_time(cls._pre_market_session[0][0],
                               cls._pre_market_session[0][1],
                               **kwargs)

    @classmethod
    def open_time(cls, **kwargs):
        return cls.change_time(cls._market_session[0][0],
                               cls._market_session[0][1],
                               **kwargs)

    @classmethod
    def break_time(cls, **kwargs):
        return cls.change_time(cls._market_break_session[0][0],
                               cls._market_break_session[0][1],
                               **kwargs)

    @classmethod
    def close_time(cls, **kwargs):
        return cls.change_time(cls._market_session[1][0],
                               cls._market_session[1][1],
                               **kwargs)

    def __repr__(self):
        return self.__class__.__name__
    __str__ = __repr__


class AMEX(StockExchange):
    name = 'American Stock Exchange' # NYSE Amex Equities
    currency = ('$', 'USD')
    timezone = 'US/Eastern'
    _market_session = ((9, 30), (16, 0))


class HK(StockExchange):
    name = 'Hong Kong Stock Exchange'
    currency = ('$', 'HKD')
    timezone = 'Asia/Shanghai'
    _pre_market_session = ((9, 30), (9, 50))
    _market_session = ((10, 0), (16, 0))
    _market_break_session = ((12, 0), (13, 30))


class LON(StockExchange):
    name = 'London Stock Exchange'
    currency = ('$', 'GBX')
    timezone = 'Europe/London'
    _market_session = ((9, 0), (17, 0))


class NASDAQ(StockExchange):
    name = 'NASDAQ Stock Exchange'
    currency = ('$', 'USD')
    timezone = 'US/Eastern'
    _market_session = ((9, 30), (16, 0))


class NYSE(StockExchange):
    name = 'New York Stock Exchange'
    currency = ('$', 'USD')
    timezone = 'US/Eastern'
    _market_session = ((9, 30), (16, 0))


class NYSEARCA(NYSE):
    pass


class SH(StockExchange):
    name = 'Shanghai Stock Exchange'
    currency = ('¥', 'CNY')
    timezone = 'Asia/Shanghai'
    _pre_market_session = ((9, 15), (9, 25))
    _market_session = ((9, 30), (15, 0))
    _market_break_session = ((11, 30), (13, 0))

    # Daily minute data count.
    market_minutes = 242


class SZ(SH):
    timezone = 'Asia/Shanghai'
    name = 'Shenzhen Stock Exchange'


class TYO(StockExchange):
    name = 'Tokyo Stock Exchange'
    currency = ('¥', 'JPY')
    timezone = 'Asia/Tokyo'
    _market_session = ((9, 0), (15, 0))
    

class YahooNA(StockExchange):
    name = 'Exchange N/A for Yahoo!'
    currency = ('$', 'USD') #default to usd
    timezone = "GMT" #default to GMT

    def __str__(self):
        return ""
    
class Security(object):
    """Finance securities includes:
    - stocks
    - stock indexes
    - funds/mutual funds
    - options
    - bonds
    """
    modules = sys.modules[__name__]

    __slots__ = ['exchange', 'symbol', 'name']

    def __init__(self, exchange, symbol, name=None):
        assert isinstance(exchange, StockExchange), "Wrong exchange."
        self.exchange = exchange
        self.symbol = symbol
        self.name = name

    def __eq__(self, other):
        return self.exchange == other.exchange and \
            self.symbol == other.symbol

    def __getstate__(self):
        return self.exchange, self.symbol

    def __setstate__(self, state):
        self.exchange, self.symbol = state

    def __repr__(self):
        args = []
        args.append("%s()" % self.exchange)
        args.append("'%s'" % self.symbol)
        if self.name:
            args.append("'%s'" % self.name)
        return "%s(%s)" % (self.__class__.__name__,
                           ', '.join(args))

    def __str__(self):
        """Symbol with exchange abbr (pre)suffix.
        """
        return "%s:%s" % (self._abbr, self.symbol)

    @classmethod
    def from_security(cls, security):
        """Helper method for convert from different services adapter."""
        assert isinstance(security, Security)
        return cls(security.exchange,
                   security.symbol,
                   security.name)

    @classmethod
    def from_abbr(cls, abbr, symbol, name=None):
        ex = getattr(cls.modules, abbr)
        return cls(ex(), symbol, name)
        
    @property
    def _abbr(self):
        """Symbol with exchange abbr suffix.
        """
        return str(self.exchange)
