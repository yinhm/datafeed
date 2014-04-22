#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2010 yinhm

'''网际风数据接口实现

接口
===
网际风接口兼容通视协议，通视协议是一个企业规范，因为出现的最早且使用广泛，逐渐
成为默认的行业标准。

通视协议是点播方式，网际风数据源与此不同，采用了全推方式。全推方式接更适合大量
数据更新。

网际风协议在通视基础上增加了盘口，除权，财务等数据。

接口调用方式参考文档：
分析家通视规范： http://www.51wjf.com/stkdrv.txt
网际风规范： http://www.51wjf.com/wjffun.txt

实现
===
网际风分客户端和stock.dll两部分，使用python ctypes加载stock.dll触发网际风客户端
自动运行，登陆服务器，接收数据。网际风数据采用推送方式返回给
stock.dll，stock.dll接收到数据后使用windows message通知监听程序（如本脚本），监
听程序根据message中的信息不同处理相应数据。
'''

import os
import sys
import thread
import time

import win32api
import win32con
import win32gui
import winerror

from ctypes import *
from ctypes.wintypes import *

from datetime import datetime

import numpy as np

from datafeed.client import Client


RCV_WORK_SENDMSG = 4

RCV_TICK = 0x3f001234
RCV_FILEDATA = 0x3f001235

STKLABEL_LEN = 10  # 股号数据长度,国内市场股号编码兼容钱龙
STKNAME_LEN = 32   # 股名长度
MAX_PATH = 260     # http://msdn.microsoft.com/en-us/library/aa365247(VS.85).aspx#maxpath


FILE_HISTORY_EX = 2  # 补日线数据
FILE_MINUTE_EX = 4   # 补分钟线数据
FILE_POWER_EX = 6    # 补充除权数据


# 下列2条补数据类型为网际风新增的扩充类型，通视协议中并未包含下述类型：
FILE_5MINUTE_EX=0x51  # 补5分钟K线  数据格式与日线完全相同 仅仅参数不同而已
FILE_1MINUTE_EX=0x52  # 补1分钟K线  数据格式与日线完全相同 仅仅参数不同而已

FILE_BASE_EX = 0x1000  # 钱龙兼容基本资料文件,m_szFileName仅包含文件名
FILE_NEWS_EX = 0x1002  # 新闻类,其类型由m_szFileName中子目录名来定
FILE_HTML_EX = 0x1004  # HTML文件,m_szFileName为URL

FILE_SOFTWARE_EX = 0x2000  # 升级软件

# 上海市场
MARKET_SH = 18515
# 深圳市场
MARKET_SZ = 23123


def format_market(value):
    if value == MARKET_SH:
        return 'SH'
    elif value == MARKET_SZ:
        return 'SZ'
    else:
        raise Exception('Unknown market.')


class Tick(Structure):
    '''tagRCV_TICK_STRUCTExV3 data structure
    '''
    _pack_ = 1
    _fields_ = [('m_cbSize', WORD),
                ('m_time', c_int),  # time_t结构
                ('m_wMarket', WORD),
                ('m_szLabel', c_char * STKLABEL_LEN),  # 股票代码,以'\0'结尾
                ('m_szName', c_char * STKNAME_LEN),    # 股票名称,以'\0'结尾

                ('m_fLastClose', c_float),
                ('m_fOpen', c_float),
                ('m_fHigh', c_float),
                ('m_fLow', c_float),
                ('m_fNewPrice', c_float),
                ('m_fVolume', c_float),
                ('m_fAmount', c_float),

                ('m_fBuyPrice', c_float * 3),
                ('m_fBuyVolume', c_float * 3),
                ('m_fSellPrice', c_float * 3),
                ('m_fSellVolume', c_float * 3),

                ('m_fBuyPrice4', c_float),
                ('m_fBuyVolume4', c_float),
                ('m_fSellPrice4', c_float),
                ('m_fSellVolume4', c_float),

                ('m_fBuyPrice5', c_float),
                ('m_fBuyVolume5', c_float),
                ('m_fSellPrice5', c_float),
                ('m_fSellVolume5', c_float)]


    @property
    def symbol(self):
        return format_market(self.m_wMarket) + self.m_szLabel

    def is_valid(self):
        """Is this tick data valid?

        We seems get data full of zero if stock got suspended.
        Use this method to detect is the data valid so you can filter it.
        """
        return self.m_fNewPrice > 0

    def to_dict(self):
        '''Convert to dict object.
        '''
        t = datetime.fromtimestamp(self.m_time)
        t = t.strftime('%Y-%m-%d %H:%M:%S')

        quote = {
            'time'     : t,
            'timestamp': self.m_time,
            'price'    : self.m_fNewPrice,
            'amount'   : self.m_fAmount,
            'volume'   : self.m_fVolume,
            'symbol'   : self.symbol,
            'name'     : self.m_szName.decode('gbk'),
            'open'     : self.m_fOpen,
            'high'     : self.m_fHigh,
            'low'      : self.m_fLow,
            'close'    : self.m_fNewPrice,
            'preclose' : self.m_fLastClose
            }
        return quote


class Head(Structure):
    '''头数据'''
    _fields_ = [('m_dwHeadTag', DWORD),
                ('m_wMarket', WORD),
                ('m_szLabel', c_char * STKLABEL_LEN)]


class History(Structure):
    '''补充日线数据'''

    _fields_ = [('m_time', c_int),
                ('m_fOpen', c_float),
                ('m_fHigh', c_float),
                ('m_fLow', c_float),
                ('m_fClose', c_float),
                ('m_fVolume', c_float),
                ('m_fAmount', c_float),
                ('m_wAdvance', WORD),
                ('m_wDecline', WORD)]

    def to_tuple(self):
        """Convert ohlc to tuple.

        Returns
        -------
        tuple
        """
        return (self.m_time,
                self.m_fOpen,
                self.m_fHigh,
                self.m_fLow,
                self.m_fClose,
                self.m_fVolume,
                self.m_fAmount)


class HistoryUnion(Union):
    '''日线数据头 or 日线数据'''

    _fields_ = [('data', History),
                ('head', Head)]

    DTYPE = [('time', '<i4'),
             ('open', '<f4'),
             ('high', '<f4'),
             ('low', '<f4'),
             ('close', '<f4'),
             ('volume', '<f4'),
             ('amount', '<f4')]

    def market(self):
        return format_market(self.head.m_wMarket)

    def symbol(self):
        return self.head.m_szLabel


class Minute(Structure):
    _fields_ = [('m_time', c_int),
                ('m_fPrice', c_float),
                ('m_fVolume', c_float),
                ('m_fAmount', c_float)]

    def to_tuple(self):
        """Convert Minute to tuple.

        Returns
        -------
        tuple
        """
        return (self.m_time,
                self.m_fPrice,
                self.m_fVolume,
                self.m_fAmount)


class MinuteUnion(Union):
    '''补充分时数据'''

    _fields_ = [('data', Minute),
                ('head', Head)]

    DTYPE = [('time', '<i4'),
             ('price', '<f4'),
             ('volume', '<f4'),
             ('amount', '<f4')]

    def market(self):
        return format_market(self.head.m_wMarket)

    def symbol(self):
        return self.head.m_szLabel


class Dividend(Union):
    pass


class FileHead(Structure):
    _fields_ = [('m_dwAttrib', DWORD),
                ('m_dwLen', DWORD),
                ('m_dwSerialNo', DWORD),
                ('m_szFileName', c_char * MAX_PATH)]


class ReceiveDataUnion(Union):
    _fields_ = [('m_pTickV3', Tick),
                ('m_pDay', HistoryUnion),
                ('m_pMinute', MinuteUnion),
                ('m_pPower', Dividend),
                ('m_pData', c_void_p)]


class ReceiveData(Structure):
    _fields_ = [('m_wDataType', c_int),
                ('m_nPacketNum', c_int),
                ('m_File', FileHead),
                ('m_bDISK', c_bool),
                ('ptr', c_int)]


class MainWindow(object):
    _WM_USER_STOCK_DATA = win32con.WM_USER + 10

    def __init__(self, host='localhost', password=None):
        self.client = Client(host=host, password=password, socket_timeout=10)

        msg_task_bar_restart = win32gui.RegisterWindowMessage("TaskbarCreated")
        message_map = {
            msg_task_bar_restart: self._on_restart,
            win32con.WM_DESTROY: self._on_destroy,
            win32con.WM_COMMAND: self._on_command,
            self._WM_USER_STOCK_DATA: self._on_data_receive,
            win32con.WM_USER+20: self._on_taskbar_notify
            }
        # Register the Window class.
        wc = win32gui.WNDCLASS()
        hinst = wc.hInstance = win32api.GetModuleHandle(None)
        wc.lpszClassName = "StockTaskBar"
        wc.style = win32con.CS_VREDRAW | win32con.CS_HREDRAW
        wc.hCursor = win32api.LoadCursor(0, win32con.IDC_ARROW)
        wc.hbrBackground = win32con.COLOR_WINDOW
        wc.lpfnWndProc = message_map  # could also specify a wndproc.

        # Don't blow up if class already registered to make testing easier
        try:
            classAtom = win32gui.RegisterClass(wc)
        except win32gui.error, err_info:
            if err_info.winerror!=winerror.ERROR_CLASS_ALREADY_EXISTS:
                raise

        # Create the Window.
        style = win32con.WS_OVERLAPPED | win32con.WS_SYSMENU
        self.hwnd = win32gui.CreateWindow(wc.lpszClassName, "WJF Data Processer", style,
                                          0, 0, win32con.CW_USEDEFAULT, win32con.CW_USEDEFAULT,
                                          0, 0, hinst, None)
        win32gui.UpdateWindow(self.hwnd)
        self._do_create_icons()
        self._do_stock_quote()

        self.periodic_wrapper(30)

    def Timer(self, timeout):
        time.sleep(timeout)
        self._on_time()
        self.periodic_wrapper(timeout)

    def periodic_wrapper(self, timeout):
        # Thread needed because win32gui does not expose SetTimer API
        thread.start_new_thread(self.Timer, (timeout, ))

    def _on_time(self):
        d = datetime.today()
        if d.hour == 15 and d.minute == 3:
            # make sure we are not receiving ticking data after market closed.
            print("Market closed, exit on %d:%d." % (d.hour, d.minute))
            win32gui.PostMessage(self.hwnd, win32con.WM_COMMAND, 1025, 0)

    def _do_stock_quote(self):
        self.stockdll = windll.LoadLibrary('C:\Windows\System32\Stock.dll')

        ret = self.stockdll.Stock_Init(self.hwnd,
                                       self._WM_USER_STOCK_DATA,
                                       RCV_WORK_SENDMSG)

        if ret != 1:
            raise Exception("Stock Init failed.")

    def _do_create_icons(self):
        # Try and find a custom icon
        hinst = win32api.GetModuleHandle(None)
        iconPathName = os.path.abspath(os.path.join(
                os.path.split(sys.executable)[0], "pyc.ico"))
        if not os.path.isfile(iconPathName):
            # Look in DLLs dir, a-la py 2.5
            iconPathName = os.path.abspath(os.path.join(
                    os.path.split(sys.executable)[0], "DLLs", "pyc.ico" ))

        if not os.path.isfile(iconPathName):
            # Look in the source tree.
            iconPathName = os.path.abspath(os.path.join(
                    os.path.split(sys.executable)[0], "..\\PC\\pyc.ico" ))

        if os.path.isfile(iconPathName):
            icon_flags = win32con.LR_LOADFROMFILE | win32con.LR_DEFAULTSIZE
            hicon = win32gui.LoadImage(hinst, iconPathName, win32con.IMAGE_ICON, 0, 0, icon_flags)
        else:
            print "Can't find a Python icon file - using default"
            hicon = win32gui.LoadIcon(0, win32con.IDI_APPLICATION)

        flags = win32gui.NIF_ICON | win32gui.NIF_MESSAGE | win32gui.NIF_TIP
        nid = (self.hwnd, 0, flags, win32con.WM_USER+20, hicon, "Python Demo")
        try:
            win32gui.Shell_NotifyIcon(win32gui.NIM_ADD, nid)
        except win32gui.error:
            # This is common when windows is starting, and this code is hit
            # before the taskbar has been created.
            print "Failed to add the taskbar icon - is explorer running?"
            # but keep running anyway - when explorer starts, we get the
            # TaskbarCreated message.

    def _on_restart(self, hwnd, msg, wparam, lparam):
        self._do_create_icons()

    def _on_destroy(self, hwnd, msg, wparam, lparam):
        nid = (self.hwnd, 0)
        win32gui.Shell_NotifyIcon(win32gui.NIM_DELETE, nid)
        win32gui.PostQuitMessage(0)  # Terminate the app.

    def _on_data_receive(self, hwnd, msg, wparam, lparam):
        header = ReceiveData.from_address(lparam)

        if wparam == RCV_TICK:
            # Tick
            records = {}
            for i in xrange(header.m_nPacketNum):
                r = Tick.from_address(header.ptr + sizeof(Tick) * i)
                if r.is_valid():
                    records[r.symbol] = r.to_dict()

            self.client.put_ticks(records)
            print "%d tick data sended" % header.m_nPacketNum
        elif wparam == RCV_FILEDATA:
            if header.m_wDataType in (FILE_HISTORY_EX, FILE_5MINUTE_EX, FILE_1MINUTE_EX):
                # Daily history
                history_head = HistoryUnion.from_address(header.ptr)

                records = []
                key = history_head.market() + history_head.symbol()
                for i in xrange(header.m_nPacketNum - 1):
                    # start from ptr + sizeof(History), first one was the header
                    q = History.from_address(header.ptr + sizeof(History) * (i+1))
                    records.append(q.to_tuple())

                rec = np.array(records, dtype=HistoryUnion.DTYPE)

                if header.m_wDataType == FILE_HISTORY_EX:
                    self.client.put_day(key, rec)
                elif header.m_wDataType == FILE_5MINUTE_EX:
                    self.client.put_5minute(key, rec)
                elif header.m_wDataType == FILE_1MINUTE_EX:
                    self.client.put_1minute(key, rec) # no implementation
            elif header.m_wDataType == FILE_MINUTE_EX:
                # Minute
                minute_head = MinuteUnion.from_address(header.ptr)

                records = []
                key = minute_head.market() + minute_head.symbol()
                for i in xrange(header.m_nPacketNum - 1):
                    # start from ptr + sizeof(Minute), first one was the header
                    q = Minute.from_address(header.ptr + sizeof(Minute) * (i+1))
                    records.append(q.to_tuple())

                rec = np.array(records, dtype=MinuteUnion.DTYPE)
                self.client.put_minute(key, rec)
            elif header.m_wDataType == FILE_POWER_EX:
                print "power ex"
            elif header.m_wDataType == FILE_BASE_EX:
                print "base ex"
            elif header.m_wDataType == FILE_NEWS_EX:
                print "news ex"
            elif header.m_wDataType == FILE_HTML_EX:
                print "html ex"
            elif header.m_wDataType == FILE_SOFTWARE_EX:
                print "software ex"
            else:
                print "Unknown file data."
        else:
            print "Unknown data type."
        return 1

    def _on_taskbar_notify(self, hwnd, msg, wparam, lparam):
        if lparam == win32con.WM_LBUTTONUP or lparam == win32con.WM_RBUTTONUP:
            print "You right clicked me."
            menu = win32gui.CreatePopupMenu()
            win32gui.AppendMenu(menu, win32con.MF_STRING, 1023, "Display Dialog")
            win32gui.AppendMenu(menu, win32con.MF_STRING, 1025, "Exit program" )
            pos = win32gui.GetCursorPos()
            # See http://msdn.microsoft.com/library/default.asp?url=/library/en-us/winui/menus_0hdi.asp
            win32gui.SetForegroundWindow(self.hwnd)
            win32gui.TrackPopupMenu(menu, win32con.TPM_LEFTALIGN, pos[0], pos[1], 0, self.hwnd, None)
            win32gui.PostMessage(self.hwnd, win32con.WM_NULL, 0, 0)
        return 1

    def _on_command(self, hwnd, msg, wparam, lparam):
        id = win32api.LOWORD(wparam)
        if id == 1023:
            print "Goodbye"
        elif id == 1025:
            print "Goodbye"
            win32gui.DestroyWindow(self.hwnd)
        else:
            print "Unknown command -", id


def program_running():
    '''Check if tongshi client is running.

    python has no method to change windows procname,
    so we check python and double check if WJFMain running too, since WJFMain
    auto exit when client exit.
    '''
    cmd = os.popen('tasklist')
    x = cmd.readlines()
    for y in x:
        p = y.find('WJFMain')
        if p >= 0:
            return True
    return False


def run_tongshi_win(server_addr='localhost', server_password=None):
    if program_running():
        print "already running"
        exit(0)

    w=MainWindow(host=server_addr, password=server_password)
    win32gui.PumpMessages()

if __name__=='__main__':
    run_tongshi_win()
