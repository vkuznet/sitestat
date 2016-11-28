#!/bin/bash
# Author: V. Kuznetsov

usage="Usage: pbr_avg.sh <fromdate todate>"
if [ "$1" == "-h" ] || [ "$1" == "-help" ] || [ "$1" == "--help" ]; then
    echo $usage
    exit 1
fi
if [ $# -ne 2 ]; then
    echo $usage
    exit 2
fi

# load PhedexReplicaMonitoring environment
source /data/srv/current/apps/PhedexReplicaMonitoring/etc/profile.d/init.sh

script=$PHEDEXREPLICAMONITORING_ROOT/bin/pbr.sh
fromdate=$1 # YYYY-MM-DD
todate=$2 # YYYY-MM-DD
hdir=hdfs:///cms/phedex-monitoring-test/$1_$2
hadoop fs -rm -f $hdir
hadoop fs -mkdir $hdir
keys=node_name,node_kind,node_tier,dataset_name,br_user_group
results=br_node_bytes

export SPARK_HOME=/usr/lib/spark
export PYTHONUNBUFFERED=1
export JAVA_JDK_ROOT
export JAVA_HOME=$JAVA_JDK_ROOT
export PBR_CONFIG=$PHEDEXREPLICAMONITORING_ROOT/etc

# kerberos
export PRMKEYTAB=/data/wma/PhedexReplicaMonitoring/prm.keytab
principal=`klist -k $PRMKEYTAB | tail -1 | awk '{print \$2}'`
echo "klist -k $PRMKEYTAB | tail -1 | awk '{print \$2}'"
kinit $principal -k -t $PRMKEYTAB

# GRID
export X509_USER_PROXY=$STATEDIR/proxy/proxy.cert
export X509_USER_CERT=$X509_USER_PROXY
export X509_USER_KEY=$X509_USER_PROXY

# PRB
#export PBR_DATA=/data/wma/PhedexReplicaMonitoring/data/
export PBR_DATA=$PHEDEXREPLICAMONITORING_ROOT/data/
wdir=$PHEDEXREPLICAMONITORING_ROOT/bin
echo "Start from $wdir"
wroot=`python -c "import ReplicaMonitoring; print '/'.join(ReplicaMonitoring.__file__.split('/')[:-1])"`
echo "wroot=$wroot"
export PYTHONPATH=/usr/lib/spark/python:$PYTHONPATH

cmd="$script --yarn --fromdate $fromdate --todate $todate --keys $keys --results $results --aggregations avg-day --fout $hdir/output"
echo "$cmd"
$cmd
