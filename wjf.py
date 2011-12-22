#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2011 yinhm

import config

from datafeed.providers.tongshi import run_tongshi_win


if __name__=='__main__':
    run_tongshi_win(config.SERVER_ADDR, config.AUTH_PASSWORD)
