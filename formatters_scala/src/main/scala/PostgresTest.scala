import Citus.{CITUS_JDBC_URL, CITUS_PROPERTIES, CITUS_TABLE}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PostgresTest {

  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("BDM2")
//      .config("spark.jars", "C:/Dev/Tools/spark-2.4.8-bin-hadoop2.7/jars/postgresql-42.3.6.jar")
//      .config("driver-class-path", "C:/Dev/Tools/spark-2.4.8-bin-hadoop2.7/jars/postgresql-42.3.6.jar")
      .config("spark.jars", "C:/Users/Victor/Downloads/postgresql-9.4.1207.jar")
      .config("driver-class-path", "C:/Users/Victor/Downloads/postgresql-9.4.1207.jar")
      .getOrCreate()
    import spark.implicits._
    val rdd:RDD[Int] = spark.sparkContext.parallelize(List(1,2,3,4,5))
    rdd.toDF("id")
      .write
      .jdbc(CITUS_JDBC_URL, "test", CITUS_PROPERTIES)
  }
}
