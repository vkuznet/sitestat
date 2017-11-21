#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : sitestats_plot.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: 
"""

# system modules
import os
import sys
import argparse

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--fin", action="store",
            dest="fin", default="", help="Input files")
        self.parser.add_argument("--fout", action="store",
            dest="fout", default="", help="Output file")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")

def values(df, bins, norm):
    out = []
    for ibin in bins:
        val = np.sum(df['bin%s' % ibin])
        out.append(val)
    return np.array(out)/norm

def plot(fin1, fin2, fin3, fout='plot.png', verbose=0):
    bins=[-1]+range(16)
    headers = ['site']+['bin%s' % i for i in bins]
    df1 = pd.read_csv(fin1, header=None, names=headers)
    df2 = pd.read_csv(fin2, header=None, names=headers)
    df3 = pd.read_csv(fin3, header=None, names=headers)
    width = 0.2
    pbyte = 1024.*1024.*1024.*1024.*1024.
    xbins = np.array(bins)+1
    fig, ax = plt.subplots()

    vals1 = values(df1, bins, pbyte)
    plt1 = ax.bar(xbins, vals1, width, color='r')
    if verbose:
        print("%s stats" % fin1)
        for k, v in zip(bins, vals1):
            print("{}: {}".format(k, v))
        print("\n")

    vals2 = values(df1, bins, pbyte)
    plt2 = ax.bar(xbins+width, vals2, width, color='y')
    if verbose:
        print("%s stats" % fin2)
        for k, v in zip(bins, vals2):
            print("{}: {}".format(k, v))
        print("\n")

    vals3 = values(df3, bins, pbyte)
    plt3 = ax.bar(xbins+2*width, vals3, width, color='g')
    if verbose:
        print("%s stats" % fin3)
        for k, v in zip(bins, vals3):
            print("{}: {}".format(k, v))
        print("\n")

    ax.set_ylabel('Size in PB')
    ax.set_title('Total size of T1+T2')
    ax.set_xticks(xbins + width / 2)
    ax.set_xticklabels(bins)
    ax.legend((plt1[0], plt2[0], plt3), ('3m', '6m', '12m'))
    plt.savefig(fout, transparent=True)
    plt.close()

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    fin1, fin2, fin3 = opts.fin.split(',')
    plot(fin1, fin2, fin3, opts.fout, opts.verbose)

if __name__ == '__main__':
    main()
