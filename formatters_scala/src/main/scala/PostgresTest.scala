import Citus.{CITUS_JDBC_URL, CITUS_PROPERTIES}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object PostgresTest {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("BDM2")
      .getOrCreate()
    import spark.implicits._
    val rdd: RDD[Int] = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5))
    rdd.toDF("id")
      .write
      .mode(SaveMode.Append)
      .jdbc(CITUS_JDBC_URL, "test", CITUS_PROPERTIES)
  }
}
