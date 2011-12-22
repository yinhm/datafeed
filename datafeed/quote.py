# -*- coding: utf-8 -*-

from datetime import datetime
from datafeed.exchange import Security

__all__ = ['Report', 'Day', 'Minute', 'SecurityList']


class _Struct(object):

    def __init__(self, security, adict):
        assert isinstance(security, Security)

        self.__dict__.update(adict)
        self.security = security

    def assert_data(self):
        pass

    def __getstate__(self):
        odict = self.__dict__.copy()
        odict.pop('_raw_data', None)
        return odict

    def __setstate__(self, state):
        self.__dict__.update(state)

    def todict(self):
        return self.__getstate__()

class Report(_Struct):

    def __init__(self, security, adict):    
        assert isinstance(adict['price'], float)
        assert isinstance(adict['time'], datetime)
        
        super(Report, self).__init__(security, adict)

    def __str__(self):
        return "%s, %s, %s" % (self.security, self.price, self.time)


class Day(_Struct):
    pass


class Minute(_Struct):
    pass

class SecurityList(_Struct):
    pass
