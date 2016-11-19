#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : merge_dbs_popdb.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: 
"""

# system modules
import os
import sys
import argparse

import pandas as pd

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--popdb", action="store",
            dest="popdb", default="", help="Input popdb file")
        self.parser.add_argument("--dbs", action="store",
            dest="dbs", default="", help="Input dbs file")
        self.parser.add_argument("--phedex", action="store",
            dest="phedex", default="", help="Input phedex file")
        self.parser.add_argument("--fout", action="store",
            dest="fout", default="", help="Output file")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")

def merge(popdb, dbs, phedex, fout):
    dbs_df=pd.read_csv(dbs).sort(['dataset'])
    print(dbs_df.head())
    pop_df=pd.read_csv(popdb).sort(['dataset'])
    print(pop_df.head())
    phe_df=pd.read_csv(phedex).sort(['dataset'])
    print(phe_df.head())
    ndf=pd.merge(dbs_df, pop_df, on='dataset', how='outer')
    all_df=pd.merge(phe_df, ndf, on='dataset', how='outer')
    all_df = all_df.replace('None', pd.np.nan)
    all_df.to_csv(fout, sep=',', na_rep=0, index=False)

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    merge(opts.popdb, opts.dbs, opts.phedex, opts.fout)

if __name__ == '__main__':
    main()
