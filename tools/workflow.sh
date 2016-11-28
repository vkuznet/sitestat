#!/bin/bash
usage="Usage: workflow.sh <ndays>"
if [ "$1" == "-h" ] || [ "$1" == "-help" ] || [ "$1" == "--help" ]; then
    echo $usage
    exit 1
fi
if [ $# -ne 1 ]; then
    echo $usage
    exit 2
fi

ndays=$1
dates1=`dates.py --ndays=$ndays`
dates2=`dates.py --ndays=$ndays --format="%Y%m%d"`

dd1=`echo $dates1 | awk '{print $1}'`
dd2=`echo $dates1 | awk '{print $2}'`
d1=`echo $dates2 | awk '{print $1}'`
d2=`echo $dates2 | awk '{print $2}'`

# popdb script
cmd_popdb="popdb.py --times=$d1-$d2 > popdb_${d1}_${d2}.csv"
echo $cmd_popdb
popdb.py --times=$d1-$d2 > popdb_${d1}_${d2}.csv

# dbs script
cmd_dbs="dbs_spark --no-log4j 2>&1 1>& dbs.log"
echo $cmd_dbs
dbs_spark --no-log4j 2>&1 1>& dbs.log

# pbr script
cmd_pbr="pbr_avg.sh $dd1 $dd2 2>&1 1>& pbr.log"
echo $cmd_pbr
pbr_avg.sh $dd1 $dd2 2>&1 1>& pbr.log

# copy files from hadoop
cmd_hadoop="hadoop fs -get /cms/phedex-monitoring-test/${dd1}_${dd2}/output ${d1}_${d2}"
echo $cmd_hadoop
hadoop fs -get /cms/phedex-monitoring-test/${dd1}_${dd2}/output ${d1}_${d2}

# make pbr dataframe
cmd_pbr_csv="make_pbr_csv.py --idir=${d1}_${d2} --fout=pbr_${d1}_${d2}.csv --group=DataOps,AnalysisOps"
echo $cmd_pbr_csv
make_pbr_csv.py --idir=${d1}_${d2} --fout=pbr_${d1}_${d2}.csv --group=DataOps,AnalysisOps

# merge data frames
cmd_merge="merge_dbs_popdb_pbr.py --popdb=popdb_${d1}_${d2}.csv --dbs=dbs_datasets.csv --phedex=pbr_${d1}_${d2}.csv --fout=data_${d1}_${d2}.csv"
echo $cmd_merge
merge_dbs_popdb_pbr.py --popdb=popdb_${d1}_${d2}.csv --dbs=dbs_datasets.csv --phedex=pbr_${d1}_${d2}.csv --fout=data_${d1}_${d2}.csv

# run stats
cmd_stats="stats.py --fin=data_${d1}_${d2}.csv --date=$d1 --nbins=15"
echo $cmd_stats
stats.py --fin=data_${d1}_${d2}.csv --date=$d1 --nbins=15

# remove logs
rm dbs.log pbr.log
