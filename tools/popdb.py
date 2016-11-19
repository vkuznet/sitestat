#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : myspark.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Example file to run basic spark job via pyspark

This code is based on example provided at
https://github.com/apache/spark/blob/master/examples/src/main/python/avro_inputformat.py

PySpark APIs:
https://spark.apache.org/docs/0.9.0/api/pyspark/index.html
"""

# system modules
import os
import sys
import imp
import pwd
import time
import json
import urllib
import urllib2
import httplib
import argparse

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        year = time.strftime("%Y", time.localtime())
        base = 'https://cmsweb.cern.ch/popdb/popularity'
        msg = "Input datasets location on HDFS, default %s" % base
        self.parser.add_argument("--base", action="store",
            dest="base", default=base, help=msg)
        msg = "time range, e.g. YYYYMMDD-YYYYMMDD"
        self.parser.add_argument("--times", action="store",
            dest="times", default="", help=msg)
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")

def x509():
    "Helper function to get x509 either from env or tmp file"
    proxy = os.environ.get('X509_USER_PROXY', '')
    if  not proxy:
        proxy = '/tmp/x509up_u%s' % pwd.getpwuid( os.getuid() ).pw_uid
        if  not os.path.isfile(proxy):
            return ''
    return proxy

class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    """
    Simple HTTPS client authentication class based on provided
    key/ca information
    """
    def __init__(self, key=None, cert=None, level=0):
        if  level > 1:
            urllib2.HTTPSHandler.__init__(self, debuglevel=1)
        else:
            urllib2.HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert

    def https_open(self, req):
        """Open request method"""
        #Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.get_connection, req)

    def get_connection(self, host, timeout=300):
        """Connection method"""
        if  self.key:
            return httplib.HTTPSConnection(host, key_file=self.key,
                                                cert_file=self.cert)
        return httplib.HTTPSConnection(host)

def getdata(url, params=None, headers=None, ckey=None, cert=None, debug=0):
    "Fetch data for given url and set of parameters"
    if  params:
        url += '?%s' % urllib.urlencode(params, doseq=True)
    if  debug:
        print("getdata:url", url)
    req = urllib2.Request(url)
    if  headers == None:
        headers = {'Accept': 'application/json'}
    if  headers:
        for key, val in headers.items():
            req.add_header(key, val)

    ckey, cert = get_key_cert()
    handler = HTTPSClientAuthHandler(ckey, cert, debug)
    opener  = urllib2.build_opener(handler)
    urllib2.install_opener(opener)
    data = urllib2.urlopen(req)
    return json.load(data)

def popdb_date(tstamp):
    "Return date in popDB format YYYY-M-D"
    if  tstamp.find('-') != -1:
        return tstamp
    if  len(tstamp)==8: # YYYYMMDD format
        year = tstamp[:4]
        month = int(tstamp[4:6])
        day = int(tstamp[6:8])
        return '%s-%s-%s' % (year, month, day)
    return tstamp

def get_key_cert():
    """
    Get user key/certificate
    """
    key  = None
    cert = None
    globus_key  = os.path.join(os.environ['HOME'], '.globus/userkey.pem')
    globus_cert = os.path.join(os.environ['HOME'], '.globus/usercert.pem')
    if  os.path.isfile(globus_key):
        key  = globus_key
    if  os.path.isfile(globus_cert):
        cert  = globus_cert

    # First presendence to HOST Certificate, RARE
    if  'X509_HOST_CERT' in os.environ:
        cert = os.environ['X509_HOST_CERT']
        key  = os.environ['X509_HOST_KEY']

    # Second preference to User Proxy, very common
    elif 'X509_USER_PROXY' in os.environ:
        cert = os.environ['X509_USER_PROXY']
        key  = cert

    # Third preference to User Cert/Proxy combinition
    elif 'X509_USER_CERT' in os.environ:
        cert = os.environ['X509_USER_CERT']
        key  = os.environ['X509_USER_KEY']

    # Worst case, look for cert at default location /tmp/x509up_u$uid
    elif not key or not cert:
        uid  = os.getuid()
        cert = '/tmp/x509up_u'+str(uid)
        key  = cert

    if  not os.path.exists(cert):
        raise Exception("Certificate PEM file %s not found" % key)
    if  not os.path.exists(key):
        raise Exception("Key PEM file %s not found" % key)

    return key, cert

def popdb(base, tstamp1, tstamp2, verbose=0):
    api = 'DSStatInTimeWindow'
    params = {'tstart':popdb_date(tstamp1), 'tstop':popdb_date(tstamp2)}
    url = '%s/%s/' % (base, api)
    res = getdata(url, params, debug=verbose)
    for row in res['DATA']:
        rec = dict(naccess=float(row['NACC']),dataset=row['COLLNAME'])
        yield rec

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    time0 = time.time()
    tstamp1, tstamp2 = opts.times.split('-')
    results = popdb(opts.base, tstamp1, tstamp2, opts.verbose)
    print("dataset,naccess")
    for row in results:
        print('%s,%s' % (row['dataset'], int(row['naccess'])))

if __name__ == '__main__':
    main()
