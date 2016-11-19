#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : make_pbr_csv.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: a helper script to create SQLiteDB from HDFS output
"""

# system modules
import os
import sys
import argparse

class OptionParser(object):
    "User based option parser"
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='mongo2hdfs')
        self.parser.add_argument("--fout", action="store",\
            dest="fout", default="pbr.csv", help="DB name")
        self.parser.add_argument("--idir", action="store",\
            dest="idir", default="", help="input dir with HDFS files")
        group = 'DataOps'
        self.parser.add_argument("--group", action="store",\
            dest="group", default=group, help="physics group, default %s" % group)
        storage = 'Disk'
        self.parser.add_argument("--storage", action="store",\
            dest="storage", default=storage, help="storage type, default %s" % storage)

def create(idir, fout, storage, phgroup):
    "Creation function"
    headers = ['node', 'kind', 'tier', 'dataset', 'phgroup', 'phedex_size']
    with open(fout, 'w') as ostream:
        ostream.write('dataset,site,phedex_size\n')
        for fname in os.listdir(idir):
            if  not fname.startswith('part'):
                continue
            print("Parsing %s" % fname)
            with open(os.path.join(idir, fname)) as istream:
                for line in istream.readlines():
                    vals = line.replace('\n', '').split(',')
                    row = dict(zip(headers, vals))
                    if  row['kind'].lower() == storage.lower() and \
                        row['phgroup'].lower() == phgroup.lower():
                        line = '%s,%s,%s' \
                                % (row['dataset'],row['node'],row['phedex_size'])
                        ostream.write(line+'\n')

def main():
    "Main function"
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    create(opts.idir, opts.fout, opts.storage, opts.group)

if __name__ == '__main__':
    main()
