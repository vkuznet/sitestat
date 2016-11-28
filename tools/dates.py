#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : dates.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: 
"""

# system modules
import os
import sys
import argparse
import datetime

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--ndays", action="store",
            dest="ndays", default=30, help="Number of days, default 30")
        iformat = '%Y-%m-%d'
        self.parser.add_argument("--format", action="store",
            dest="format", default=iformat, help="date format, %s" % iformat)

def dates(numdays):
    base = datetime.datetime.today()
    date_list = [base - datetime.timedelta(days=x) for x in range(0, numdays)]
    return date_list

def dformat(date, iformat):
    return date.strftime(iformat)

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    date_list = dates(int(opts.ndays))
    min_date, max_date = date_list[-1], date_list[0]
    print('%s %s' % (dformat(min_date, opts.format), dformat(max_date, opts.format)))

if __name__ == '__main__':
    main()
