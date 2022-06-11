import json

from pyspark import SparkConf, SQLContext, SparkContext
from pyspark.sql import SparkSession

conf = (SparkConf().setMaster("spark://clefable.fib.upc.es:7077").setAppName("RW_from_HBase"))

sc = SparkContext(conf=conf)

conf = {
        "hbase.zookeeper.quorum": "node1,node2,node3",
        "hbase.mapreduce.inputtable": "test",
        "hbase.mapreduce.scan.row.start": "1541692800",
        "hbase.mapreduce.scan.row.stop": "1541692900"
    }
keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"
hbaserdd = sc.newAPIHadoopRDD(
    "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
    "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
    "org.apache.hadoop.hbase.client.Result",
    keyConverter=keyConv,
    valueConverter=valueConv,
    conf=conf)
spark = SparkSession(sc)

print(hbaserdd.count())