from __future__ import with_statement

import datetime
import os
import unittest

from datafeed.exchange import *
from datafeed.providers.nasdaq import *


class NasdaqSecurityTest(unittest.TestCase):

    def test_str(self):
        s = NasdaqSecurity(NYSE(), 'MMM')
        self.assertEqual(str(s), 'NYSE:MMM')
    

class NasdaqListTest(unittest.TestCase):
    _RAW_DATA = '''"Symbol","Name","LastSale","MarketCap","IPOyear","Sector","Industry","Summary Quote",
"MMM","3M Company","91.97","65351766690","n/a","Health Care","Medical/Dental Instruments","http://quotes.nasdaq.com/asp/SummaryQuote.asp?symbol=MMM&selected=MMM",
"SVN","7 Days Group Holdings Limited","18.6","345048600","2009","Consumer Services","Hotels/Resorts","http://quotes.nasdaq.com/asp/SummaryQuote.asp?symbol=SVN&selected=SVN",
"NDN","99 Cents Only Stores","20.2","1415515000","1996","Consumer Services","Department/Specialty Retail Stores","http://quotes.nasdaq.com/asp/SummaryQuote.asp?symbol=NDN&selected=NDN",
"AHC","A.H. Belo Corporation","6.83","130575940","n/a","Consumer Services","Newspapers/Magazines","http://quotes.nasdaq.com/asp/SummaryQuote.asp?symbol=AHC&selected=AHC",'''


    def test_nasdaq_report(self):
        ret = NasdaqList.parse(NYSE(), self._RAW_DATA)
        i = 0
        for r in ret:
            if i == 0:
                self.assertEqual(r.security.exchange, NYSE())
                self.assertEqual(r.security.symbol, 'MMM')
                self.assertEqual(r.name, "3M Company")
                self.assertEqual(r.price, 91.97)

            if i == 1:
                self.assertEqual(r.security.exchange, NYSE())
                self.assertEqual(r.security.symbol, 'SVN')

            i += 1

        self.assertEqual(i, 4)


class NasdaqListFetcherTest(unittest.TestCase):

    def test_init(self):
        f = NasdaqListFetcher()
        self.assertEqual(f._base_url,
                         'http://www.nasdaq.com/screening/companies-by-industry.aspx')

    def test_fetch_with_wrong_arguments(self):
        f = NasdaqListFetcher()
        self.assertRaises(AssertionError, f.fetch, SH())


if __name__ == '__main__':
    unittest.main()
