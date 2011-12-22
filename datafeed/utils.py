#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2010 yinhm

import datetime
import json

import numpy as np

from json import encoder
encoder.FLOAT_REPR = lambda f: format(f, '.2f')


__all__ = ['print2f', 'json_encode', 'json_decode']


class print2f(float):
    def __repr__(self):
        return "%0.2f" % self


def json_encode(value):
    """JSON-encodes the given Python object."""
    # JSON permits but does not require forward slashes to be escaped.
    # This is useful when json data is emitted in a <script> tag
    # in HTML, as it prevents </script> tags from prematurely terminating
    # the javscript.  Some json libraries do this escaping by default,
    # although python's standard library does not, so we do it here.
    # http://stackoverflow.com/questions/1580647/json-why-are-forward-slashes-escaped
    return json.dumps(value).replace("</", "<\\/")


def json_decode(value):
    """Returns Python objects for the given JSON string."""
    return json.loads(value)
