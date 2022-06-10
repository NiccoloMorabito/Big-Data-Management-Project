import it.nerdammer.spark.hbase._
import org.apache.spark.sql.SparkSession

object Peru {
  def format(spark: SparkSession): Unit = {
    import spark.implicits._
    val categories = Citus.getCategories(spark).select("detail_code").collect()

    val data = spark.sparkContext.hbaseTable[(String, String, Float, String, Float, String)]("peru")
      .select("FECH_RECEP", "PAIS_ORIGE", "PESO_NETO", "UNID_FIDES", "UNID_FIQTY", "DESC_COMER")
      .inColumnFamily("values")

    val exports = data
      .withStartRow("x")
      .withStopRow("y")

    val imports = data
      .withStartRow("mb")
      .withStopRow("mc")


    val impDF = imports
      .map(row => (
        row._2, // origin country
        "Peru", // destination country
        row._1, // transaction date
        row._3, // price (only net price)
        row._4, // unit
        row._5, // amount
        "", // product_category TODO
        row._6 // description
      ))
      .toDF("origin", "destination", "transaction_date", "price", "unit", "quantity", "product_category", "description")

    Citus.appendData(impDF)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("BDM2")
      .config("spark.hbase.host", "victreebel.fib.upc.es")
      .config("spark.hbase.port", "27000")
      .getOrCreate()

    format(spark)

    spark.stop()
  }
}
