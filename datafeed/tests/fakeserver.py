#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2014 yinhm
import atexit
import logging
import tornado
import os
import signal
import shutil

from tornado import ioloop
from tornado.options import define, options

from datafeed.exchange import SH
from datafeed.imiguserver import ImiguApplication
from datafeed.server import Server

import time

def main():
    datadir = '/tmp/datafeed-%d' % int(time.time() * 1000 * 1000)
    os.mkdir(datadir)

    tornado.options.parse_command_line()
    app = ImiguApplication(datadir, SH(), rdb=True)
    server = Server(app)
    server.listen(55555)
    io_loop = tornado.ioloop.IOLoop.instance()

    def shutdown(signum, frame):
        io_loop.stop()
        shutil.rmtree(datadir, ignore_errors=True)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    io_loop.start()

if __name__ == "__main__":
    main()
