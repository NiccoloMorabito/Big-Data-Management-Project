#!bin/bash

echo "Starting HDFS"
~/BDM_Software/hadoop/sbin/start-dfs.sh

echo "Creating user folder"
~/BDM_Software/hadoop/bin/hdfs dfs -mkdir /user
~/BDM_Software/hadoop/bin/hdfs dfs -mkdir /user/bdm
~/BDM_Software/hadoop/bin/hdfs dfs -chmod -R 777 /user/bdm/

