#!/usr/bin/env python
"""A bidirectional dict.
"""
import itertools

class Bidict(dict):
    def __init__(self, iterable=(), **kwargs):
        self.update(iterable, **kwargs)

    def update(self, iterable=(), **kwargs):
        if hasattr(iterable, 'iteritems'):
            iterable = iterable.iteritems()
        for (key, value) in itertools.chain(iterable, kwargs.iteritems()):
            self[key] = value

    def __setitem__(self, key, value):
        if key in self:
            del self[key]
        if value in self:
            del self[value]
        dict.__setitem__(self, key, value)
        dict.__setitem__(self, value, key)

    def __delitem__(self, key):
        value = self[key]
        dict.__delitem__(self, key)
        dict.__delitem__(self, value)

    def __repr__(self):
        return '%s(%s)' % (type(self).__name__, dict.__repr__(self))
