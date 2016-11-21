# Introduction
This folder contains set of python scripts to acquire information
from different CMS services, such as DBS, Phedex and PopDB.
The former contains information about dataset and their size and number of
events. The Phedex contains information about dataset replicas, while
PopDB provides information about number of accesses to datasets.
Below we outline procedure how to find summary information about
T1+T2 sites, i.e. total size of datasets on them, e.g. see
![T1+T2 site stats](https://github.com/vkuznet/sitestat/blob/master/tools/T1_T2_stats.png)

## Step by step instructions

First, we need to call popDB to get information about datasets' accesses
for given period of time:

```
  popdb.py --times=20160724-20161024 > popdb_20160724_20161024.csv
```

Then, we extract dataset information, such as their size and creation
time from DBS data stored on HDFS. This step should be done on a node
which can submit a spark job:

```
  dbs_spark --no-log4j 2>&1 1>& log
  cat log | > dbs_datasets.csv
```

To get information about dataset replicas we need to use pbr
(Phedex Block Replica) script and run it again on a spark:

```
pbr_avg.sh 2016-07-24 2016-10-24 2>&1 1>& log
```

It stores data on hdfs:///cms/phedex-monitoring-test (you
may adjust this parameter in pbr.sh). To get the data from HDFS we run
the following command:

```
hadoop fs -get /cms/phedex-monitoring-test/SPARK_DIR ./20160724_20161024
```

and then merge spark files into single CSV one:

```
make_pbr_csv.py --idir=20160724_20161024 --fout=pbr_20160724_20161024.csv
```

Finally, we merge data from PopDB, DBS and Phedex system into a single
dataframe:

```
merge_dbs_popdb_pbr.py \
   --popdb=popdb_20160724_20161024.csv \
   --dbs=dbs_datasets.csv \
   --phedex=pbr_20160724_20161024.csv --fout=data.csv
```

and we run stats script to get final statistics:

```
stats.py --fin=data.csv.gz --date=20160724 --nbins=15
```

The stats.py script does the following:

- merge data from PopDB, DBS, Phedex csv files into a single dataframe
  using outer join on dataset column [1]
- parition data into bins
  - zero-old bin where creation_time was before time window, e.g. older than 3m
  - zero-new bin where creation_time was after time window
  - non-zero bins will come from merged popdb+DBS file

[1] http://pandas.pydata.org/pandas-docs/stable/merging.html#database-style-dataframe-joining-merging
