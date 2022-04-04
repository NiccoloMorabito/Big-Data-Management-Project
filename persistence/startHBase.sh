#! /bin/bash

echo "Starting HDFS ..."
~/BDM_Software/hadoop/sbin/start-dfs.sh
echo "HDFS started"
echo "Starting HBase ..."
~/BDM_Software/hbase/bin/start-hbase.sh
echo "HBase started"
echo "Starting Thrift Server"
~/BDM_Software/hbase/bin/hbase thrift start &>~/thrift.log &
echo "Thrift Server started, logging into ~/thrift.log"
