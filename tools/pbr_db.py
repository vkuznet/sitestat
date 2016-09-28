#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : pbr_db.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: a helper script to create SQLiteDB from HDFS output
produced by PhedexReplicaMonitoring script.
https://github.com/vkuznet/PhedexReplicaMonitoring
"""

# system modules
import os
import sys
import sqlite3
import argparse

class OptionParser(object):
    "User based option parser"
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='mongo2hdfs')
        self.parser.add_argument("--dbname", action="store",\
            dest="dbname", default="pbr.db", help="DB name")
        self.parser.add_argument("--idir", action="store",\
            dest="idir", default="", help="input dir with HDFS files")

def create(dbname, idir):
    "Creation function"
    conn = sqlite3.connect(dbname)
    cur = conn.cursor()
    # Disk,/ZeroBias6/Run2016B-LumiPixelsMinBias-PromptReco-v2/ALCARECO,caf-lumi,4.4867629926E10
    cur.execute("CREATE TABLE avg (node text, dataset text, phgroup text, size real)")
    cur.execute("create index idx ON avg(dataset)")
    for fname in os.listdir(idir):
        if  not fname.startswith('part'):
            continue
        print("Parsing %s" % fname)
        with open(os.path.join(idir, fname)) as istream:
            for line in istream.readlines():
                vals = line.replace('\n', '').split(',')
                stm = "INSERT INTO avg VALUES ('{}','{}','{}',{})".format(*vals)
                cur.execute(stm)
    conn.commit()
    conn.close()

def main():
    "Main function"
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    create(opts.dbname, opts.idir)

if __name__ == '__main__':
    main()
