#!/usr/bin/env python
import unittest

TEST_MODULES = [
    'datafeed.tests.test_client',
    'datafeed.tests.test_datastore',
    'datafeed.tests.test_exchange',
    'datafeed.tests.test_imiguserver',
    'datafeed.tests.test_server',
]

def all():
    return unittest.defaultTestLoader.loadTestsFromNames(TEST_MODULES)

if __name__ == '__main__':
    import tornado.testing
    tornado.testing.main()
