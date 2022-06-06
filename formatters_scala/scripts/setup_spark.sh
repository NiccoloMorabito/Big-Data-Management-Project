#!/bin/bash

echo "Downloading Spark"
wget https://archive.apache.org/dist/spark/spark-2.4.8/spark-2.4.8-bin-hadoop2.7.tgz

echo "Extracting"
tar xvf spark-2.4.8-bin-hadoop2.7.tgz

echo "Moving sources"
sudo mv spark-2.4.8-bin-hadoop2.7/ /opt/spark

echo "Updating path"
echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.bashrc
source ~/.bashrc

echo "Spark set up"
echo "To start a master process, run start-master.sh"
echo "To start a worker process, run start-worker.sh <master url>"

