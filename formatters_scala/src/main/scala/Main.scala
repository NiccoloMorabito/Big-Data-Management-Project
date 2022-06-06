import it.nerdammer.spark.hbase._
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("BDM2")
      .config("spark.hbase.host", "victreebel.fib.upc.es")
      .config("spark.hbase.port", "27000")
      .getOrCreate()

    print(spark.sparkContext.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8)).sum())

    // Read HBase Table
    val hBaseRDD = spark.sparkContext
      .hbaseTable[(String, String)]("peru")
      .select("filename")
      .inColumnFamily("values")
    println(s"Peru table has ${hBaseRDD.count()} rows")
    // Iterate HBaseRDD and generate RDD[Row]
    //    val rowRDD = hBaseRDD.map(i => Row(i._1.get, i._2.get, i._3.get))
    //
    //    print(rowRDD.count())
  }
}