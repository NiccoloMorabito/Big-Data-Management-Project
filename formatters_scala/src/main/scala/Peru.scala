import it.nerdammer.spark.hbase._
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import scala.util.Random

object Peru {
  def format(spark: SparkSession): Unit = {
    import spark.implicits._
    val categories = Citus.getCategories(spark)
      .select("subcategory_code")
      .map(row => row.getString(0))
      .collect()
    val countries = Citus.getCountries(spark)
      .select("iso2", "iso3")
      .rdd
      .map(row => (row.getString(0), row.getString(1)))
      .collectAsMap()

    val existing_rows = Citus.getTransactions(spark)
      .select("id").map(row => row.getString(0)).collect().toSet

    val exports = spark.sparkContext.hbaseTable[(String, String, String, String, String, String, String)]("peru")
      .select("FECH_RECEP", "CPAIDES", "VPESNET", "TUNIFIS", "QUNIFIS", "DCOM")
      .inColumnFamily("values")
      .withStartRow("x")
      .withStopRow("y")
      .filter(row => !existing_rows.contains(row._1))
      .filter(row => countries.contains(row._3))
      .map(row => (
        row._1, // row id
        "PER", // origin country
        countries(row._3), // destination country
        new java.sql.Date(new SimpleDateFormat("yyyyMMdd").parse(row._2).getTime), // transaction date
        row._4.toFloat, // price (only net price)
        row._5, // unit
        row._6.toFloat, // amount
        categories(Random.nextInt(categories.length)), // product_category
        row._7 // description
      ))
      .toDF("id", "origin", "destination", "transaction_date", "price", "unit", "quantity", "product_category", "description")

    Citus.appendData(exports)

    val imports = spark.sparkContext.hbaseTable[(String, String, String, String, String, String, String)]("peru")
      .select("FECH_RECEP", "PAIS_ORIGE", "PESO_NETO", "UNID_FIDES", "UNID_FIQTY", "DESC_COMER")
      .inColumnFamily("values")
      .withStartRow("ma")
      .withStopRow("mal")
      .filter(row => !existing_rows.contains(row._1))
      .filter(row => countries.contains(row._3))
      .map(row => (
        row._1, // row id
        countries(row._3), // origin country
        "PER", // destination country
        new java.sql.Date(new SimpleDateFormat("yyyyMMdd").parse(row._2).getTime), // transaction date
        row._4.toFloat, // price (only net price)
        row._5, // unit
        row._6.toFloat, // amount
        categories(Random.nextInt(categories.length)), // product_category
        row._7 // description
      ))
      .toDF("id", "origin", "destination", "transaction_date", "price", "unit", "quantity", "product_category", "description")

    Citus.appendData(imports)
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
