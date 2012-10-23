#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2010 yinhm

"""大智慧日线数据抓换

大智慧数据格式分析：
http://hi.baidu.com/wu988/blog/item/9321c4036917a30f728da55d.html

文件路径
-------
$DZH/data/sh/day.dat

文件头
-----
起止地址   数据内容                 数据含义       数据类型
00 - 03   F4 9B 13 FC             文件标志 int
04 - 07   00 06 00 00             未知           int
08 - 0B   00 00 00 00             保留           int
0C - 0F   97 04 00 00             证券总数       int
10 - 13   00 18 00 00             未知           int 需添加之起始块号
14 - 17   DB 17 00 00             未知           int 当前最后空块号

记录块号为FFFF表示未分配.从41000h开始的8KB为第0号数据存储块.

"""

import os
import ConfigParser
import urllib2

from collections import OrderedDict
from cStringIO import StringIO
from struct import *

import h5py
import numpy as np


__all__ = ['DzhDay', 'DzhDividend',
           'DzhMinute', 'DzhFiveMinute',
           'DzhFin', 'DzhSector']


class EndOfIndexError(StandardError):
    pass

class FileNotFoundError(StandardError):
    pass


def gb2utf(value, ignore=True):
    if ignore:
        return unicode(value, 'gb18030', 'ignore').encode('utf-8', 'ignore')
    else:
        return unicode(value, 'gb18030').encode('utf-8')


class DzhDay(object):
    """大智慧日线数据"""

    _COUNT_SDTART = int('0x0c', 16)
    _INDEX_START  = int('0x18', 16)
    _BLOCK_START  = int('0x41000', 16) # OHLCs
    _BLOCK_SIZE   = 256 * 32

    _DTYPE = [('time', '<i4'),
              ('open', '<f4'),
              ('high', '<f4'),
              ('low', '<f4'),
              ('close', '<f4'),
              ('volume', '<f4'),
              ('amount', '<f4')]


    def read(self, filename, market):
        """Generator of 日线数据读取

        Block read each symbols data.

        Parameters
        ----------
        store : hdf5 store
        """
        self.f = open(filename, 'r')

        try:
            i = 0
            while True:
                self.f.seek(self._INDEX_START + 64 * i, 0)
                index = self.read_index()

                symbol = market + index[0]

                timestamps = []
                ohlcs = []
                for block in index[2]:
                    self.read_block(block=block, timestamps=timestamps, ohlcs=ohlcs)

                ohlcs = np.array(ohlcs)
                ohlcs = np.rec.fromarrays([timestamps,
                                           ohlcs[:, 0],
                                           ohlcs[:, 1],
                                           ohlcs[:, 2],
                                           ohlcs[:, 3],
                                           ohlcs[:, 4],
                                           ohlcs[:, 5]],
                                          dtype=self._DTYPE)

                yield symbol, ohlcs

                i += 1
        except (EOFError, EndOfIndexError):
            raise StopIteration
        # except Exception as e:
        #     '''locks like we got some duplicated data, eg:

        #     sz399004
        #     --------

        #     block 8846 has 4 of those:
        #     2010-05-10
        #     2010-05-11
        #     2010-05-12
        #     2010-05-13

        #     How do we fix this?
        #     '''
        finally:
            self.f.close()

    def read_index(self):
        """索引记录格式

        数据块大小
        ---------
        0x18起每64byte为一组索引数据

        数据库结构
        ---------
        18 - 21   31 41 30 30 30...FF     证券代码       byte[10]
        22 - 25   B0 09 00 00             ohlc记录数     int
        26 - 27   05 00                   第一个记录块号short
        28 - 29   06 00                   第二个记录块号 short
        56 - 57                           第25个记录块号short

        Return tuple of index

        Examples
        --------
        >>> index = read_index(f)
        >>> index
        ('000001', 4767, [0, 1132, 1135])

        """
        symbol = unpack('10s', self.f.read(10))[0].replace('\x00', '')

        if symbol == '':
            raise EOFError

        count =  unpack('i', self.f.read(4))[0]

        blocks = []

        for i in range(25):
            block_id = unpack('h',  self.f.read(2))[0]
            if block_id != -1: # 0xff 0xff
                blocks.append(block_id)

        return (symbol, count, blocks)

    def read_block(self, block, timestamps, ohlcs):
        """read ohlc data rows for a symbol

        data length
        -----------
        8KB each symbol, 256 * 32bytes

        ohlc记录格式
        -----------
        41000 - 41003 80 47 B2 2B         日期           int
        41004 - 41007 B9 1E 25 41         开盘价         float
        41008 - 4100B CD CC 4C 41         最高价         float
        4100C - 4100F EC 51 18 41         最低价         float
        41010 - 41013 9A 99 41 41         收盘价         float
        41014 - 41017 80 06 B2 47         成交量         float
        41018 - 4101B 40 1C BC 4C         成交金额       float
        4101C - 4101D 00 00               上涨家数       short
        4101E - 4101F 00 00               下跌家数       short
        日期为unixtime.

        Returns
        -------
        True on success or Error raised
        """
        try:
            self.f.seek(self._BLOCK_START + self._BLOCK_SIZE * block, 0) # reseek to block head
        except:
            print "wrong block size? %d" % block

        for i in range(256):
            rawdata = self.f.read(4)

            if rawdata == '':
                raise EOFError

            timestamp = unpack('i', rawdata)[0]
            if timestamp <= 0:
                # invalid: \x00 * 4 || \xff * 4
                self.f.seek(24, 1)
            else:
                ohlc = np.frombuffer(self.f.read(24), dtype=np.float32)

                timestamps.append(timestamp)
                ohlcs.append(ohlc)

            self.f.seek(4, 1) # skip 2*2short for rasie/up count

        return True


class DzhMinute(DzhDay):
    """大智慧1分钟数据"""
    _BLOCK_START  = int('0x41000', 16)
    _BLOCK_SIZE   = 384 * 32


class DzhFiveMinute(DzhDay):
    """大智慧5分钟数据

    IMPORTANT:

    大智慧五分钟数据时区处理有误，导致time数据相差8小时。
    数据读取未对原始数据做任何改动，实际使用中，需手工修正，eg:

        for symbol, ohlcs in io.read('MIN1.DAT', 'SH'):
            for ohlc in ohlcs:
                ohlc['time'] = ohlc['time'] - 8 * 3600
    """
    #_BLOCK_START  = int('0x41000', 16)
    #_BLOCK_SIZE   = 384 * 32


class DzhFetcher(object):
    _IPS = ('222.73.103.181', '222.73.103.183')
    _PATH = None

    def __init__(self):
        self.ips = list(self._IPS)
        self._fetched = False

    def fetch_next_server(self):
        self.ips.pop
        if len(self.ips) == 0:
            raise FileNotFoundError
        return self.fetch()

    def fetch(self):
        try:
            r = urllib2.urlopen(self.data_url())
            data = r.read()
            self.f = StringIO(data)
            self._fetched = True
        except URLError:
            return self.fetch_next_server()

    def data_url(self):
        assert self._PATH, "No file path."

        if len(self.ips) == 0:
            return None

        return "http://" + self.ips[-1] + self._PATH


class DzhDividend(DzhFetcher):
    '''大智慧除权数据'''
    _PATH = '/platform/download/PWR/full.PWR'

    def read(self):
        """Generator of 大智慧除权数据

        Example of yield data:

        symbol: 'SZ000001'
        dividends: [{ :date_ex_dividend => '1992-03-23',
                      :split => 0.500,
                      :purchase => 0.000,
                      :purchase_price => 0.000,
                      :dividend => 0.200 }... ]
        """
        if self._fetched == False:
            self.fetch()

        # skip head
        self.f.seek(12, 0)

        try:
            while True:
                yield self._read_symbol()
        except EOFError:
            raise StopIteration
        finally:
            self.f.close()
        #except Exception as e:
        #    print(e)

    def _read_symbol(self):
        dividends = []

        rawsymbol = self.f.read(16)
        if rawsymbol == '':
            raise EOFError

        symbol = unpack('16s', rawsymbol)[0].replace('\x00', '')

        rawdate = self.f.read(4)

        dt = np.dtype([('time', np.int32),
                       ('split', np.float32),
                       ('purchase', np.float32),
                       ('purchase_price', np.float32),
                       ('dividend', np.float32)])
        while (rawdate) != "\xff" * 4:
            dividend = np.frombuffer(rawdate + self.f.read(16), dtype=dt)
            dividends.append(dividend)

            rawdate = self.f.read(4)
            if rawdate == '':
                break

        return (symbol, np.fromiter(dividends, dtype=dt))



class DzhFin(DzhFetcher):
    '''大智慧财务数据'''
    _PATH = '/platform/download/FIN/full.FIN'

    def read(self):
        """Generator of 大智慧财务数据

        See _read_row for data of each iter.
        """
        if self._fetched == False:
            self.fetch()

        # skip head
        self.f.seek(8, 0)

        try:
            while True:
                yield self._read_row()
        except EOFError:
            raise StopIteration
        finally:
            self.f.close()
        #except Exception as e:
        #    print(e)

    def _read_row(self):
        rawsymbol = self.f.read(12)
        if rawsymbol == '':
            raise EOFError

        symbol = unpack('12s', rawsymbol)[0].replace('\x00', '')

        dt = np.dtype([
            ('report_date' , np.int32),   # 0  报告发布日期
            ('update_date' , np.int32),   # 1  更新日期
            ('listing_date', np.int32),   # 2  上市日期
            ('MGSY'        , np.float32), # 3  每股收益
            ('MGJZC'       , np.float32), # 4  每股净资产, 股票净值
            ('JZCSYL'      , np.float32), # 5  净资产收益率
            ('MGJYXJ'      , np.float32), # 6  每股经营现金
            ('MGGJJ'       , np.float32), # 7  每股公积金
            ('MGWFPLR'     , np.float32), # 8  每股未分配利润
            ('GDQYB'       , np.float32), # 9  股东权益比
            ('JLRTB'       , np.float32), # 10 净利润同比
            ('ZYSRTB'      , np.float32), # 11 主营收入同比
            ('XSMLR'       , np.float32), # 12 销售毛利率
            ('TZMGJZC'     , np.float32), # 13 调整每股净资产
            ('ZZC'         , np.float32), # 14 总资产
            ('LDZC'        , np.float32), # 15 流动资产
            ('GDZC'        , np.float32), # 16 固定资产
            ('WXZC'        , np.float32), # 17 无形资产
            ('LDFZ'        , np.float32), # 18 流动负债
            ('CQFZ'        , np.float32), # 19 长期负债
            ('ZFZ'         , np.float32), # 20 总负债
            ('GDQY'        , np.float32), # 21 股东权益
            ('ZBGJJ'       , np.float32), # 22 资本公积金
            ('JYXJLL'      , np.float32), # 23 经营现金流量
            ('TZXJLL'      , np.float32), # 24 投资现金流量
            ('CZXJLL'      , np.float32), # 25 筹资现金流量
            ('XJZJE'       , np.float32), # 26 现金增加额
            ('ZYSR'        , np.float32), # 27 主营收入
            ('ZYLR'        , np.float32), # 28 主营利润
            ('YYLR'        , np.float32), # 29 营业利润
            ('TZSY'        , np.float32), # 30 投资收益
            ('YYWSZ'       , np.float32), # 31 营业外收支
            ('YRZE'        , np.float32), # 32 利润总额
            ('JLR'         , np.float32), # 33 净利润
            ('WFPLR'       , np.float32), # 34 未分配利润
            ('ZGB'         , np.float32), # 35 总股本
            ('WXGGHJ'      , np.float32), # 36 无限售股合计
            ('ASHARE'      , np.float32), # 37 A股
            ('BSHARE'      , np.float32), # 38 B股
            ('JWSSG'       , np.float32), # 39 境外上市股
            ('QTLTG'       , np.float32), # 40 其他流通股
            ('XSGHJ'       , np.float32), # 41 限售股合计
            ('GJCG'        , np.float32), # 42 国家持股
            ('GYFRG'       , np.float32), # 43 国有法人股
            ('JNFRG'       , np.float32), # 44 境内法人股
            ('JNZRRG'      , np.float32), # 45 境内自然人股
            ('QTFQRG'      , np.float32), # 46 其他发起人股
            ('MJFRG'       , np.float32), # 47 募集法人股
            ('JWFRG'       , np.float32), # 48 境外法人股
            ('JWZRRG'      , np.float32), # 49 境外自然人股
            ('YXGHQT'      , np.float32), # 50 优先股或其他
            ])

        row = np.frombuffer(self.f.read(len(dt) * 4), dtype=dt)
        return (symbol, row)


_SECTORS = ('行业', '概念', '地域',
            '证监会行业', '指数板块')
class DzhSector(DzhFetcher):
    '''大智慧板块数据'''

    _PATH = '/platform/download/ABK/full.ABK'

    def read(self):
        """Generator of 大智慧板块数据
        """
        if self._fetched == False:
            self.fetch()

        content = self.f.read()
        file = StringIO()
        file.write(gb2utf(content))
        file.seek(0)

        config = ConfigParser.ConfigParser()
        config.readfp(file)

        for sector in _SECTORS:
            options = OrderedDict()
            for name, value in config.items(sector):
                options[name] = value.split(' ')
            yield sector, options

        self.f.close()
        file.close


if __name__ == '__main__':
    from cStringIO import StringIO
    from datafeed.client import Client

    client = Client()

    # path = os.path.join(os.path.realpath(os.path.dirname(__file__)),
    #                     '../../var')

    # filename = os.path.join(path, "/dzh/sh/DAY.DAT")
    # io = DzhDay()
    # for symbol, ohlcs in io.read(filename, 'SH') :
    #     memfile = StringIO()
    #     np.save(memfile, ohlcs)
    #     client.put('DayHistory', symbol, memfile.getvalue())


    io = DzhDividend()
    for data in io.read():
        memfile = StringIO()
        np.save(memfile, data[1])
        client.put('dividend', data[0], memfile.getvalue())
