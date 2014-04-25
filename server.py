#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2010 yinhm

'''A datafeed server daemon.
'''
import config
import logging
import os
import signal
import sys
import tornado

from tornado import ioloop
from tornado.options import define, options

from datafeed.exchange import SH
from datafeed.imiguserver import ImiguApplication
from datafeed.server import Server


DATA_DIR = os.path.join(os.path.realpath(os.path.dirname(__file__)),
                        'var')

define("port", default=8082, help="run on the given port", type=int)
define("datadir", default=DATA_DIR, help="default data dir", type=str)
define("rdb", default=False, help="enable rocksdb for archive data", type=bool)


def main():
    tornado.options.parse_command_line()

    app = ImiguApplication(options.datadir, SH(), rdb=options.rdb)
    server = Server(app, auth_password=config.AUTH_PASSWORD)
    server.listen(options.port)
    io_loop = tornado.ioloop.IOLoop.instance()

    check_time = 1 * 1000  # every second
    scheduler = ioloop.PeriodicCallback(app.periodic_job,
                                        check_time,
                                        io_loop=io_loop)

    def shutdown(signum, frame):
        print 'Signal handler called with signal', signum
        io_loop.stop()
        scheduler.stop()
        server.log_stats()
        logging.info("==> Exiting datafeed.")

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    scheduler.start()
    io_loop.start()

if __name__ == "__main__":
    main()
