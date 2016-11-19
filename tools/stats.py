#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : an.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: 
"""

# system modules
import os
import sys
import time
import argparse
import pandas as pd

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--fin", action="store",
            dest="fin", default="", help="Input file")
        self.parser.add_argument("--date", action="store",
            dest="date", default="", help="date cut-off, e.g. 20160724")
        self.parser.add_argument("--nbins", action="store",
            dest="nbins", default="15", help="number of bins")

def stats(fname, date, nbins=15):
    dtype = {'dataset': str, 'date': float, 'phedex_size': float, 'site':str, 'size':float, 'evt':int, 'naccess':int}
    df = pd.read_csv(fname, dtype=dtype)
    print("Input df: nrows=%s, ncols=%s" % (df.shape[0], df.shape[1]))
    df=df[df['site'].map(lambda x: str(x).startswith('T1') or str(x).startswith('T2'))]
    sites = sorted(list(df['site'].unique()))
    print("Sites: %s" % ', '.join(sites))
    bins = {}
    for ibin in range(nbins):
        if  ibin == nbins-1:
            bins[ibin] = df[df['naccess']>=ibin]
        else:
            bins[ibin] = df[df['naccess']==ibin]
    df0 = df[df['naccess']==0]
    thr = time.mktime(time.strptime(date, '%Y%m%d'))
    bins[-1] = df0[df0['date']<thr]
    bins[0] = df0[df0['date']>=thr]
    peta = pow(pow(2,10), 5) # Peta-Bytes
    for ibin in range(-1, nbins):
        print('%s %s' % (ibin, bins[ibin]['phedex_size'].sum()/peta))

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    stats(opts.fin, opts.date, int(opts.nbins))

if __name__ == '__main__':
    main()
