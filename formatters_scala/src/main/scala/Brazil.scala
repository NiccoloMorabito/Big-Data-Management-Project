import it.nerdammer.spark.hbase._
import org.apache.spark.sql.SparkSession

import java.sql.Date
import java.text.SimpleDateFormat

object Brazil {

  def brazil(spark: SparkSession): Unit = {
    import spark.implicits._
    val sc = spark.sparkContext

    val codeToCountry = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("encoding", "latin1")
      .csv("src/main/resources/PAIS.csv")
      .map(row => (row.getString(0), row.getString(2)))
      .rdd
      .collectAsMap()


    val brazilTable = sc.hbaseTable[(String, String, String, String, String, String, String)]("brazil")
      .select("CO_PAIS", "CO_ANO", "CO_MES", "VL_FOB", "KG_LIQUIDO", "SH4")
      .inColumnFamily("values")

    val expDF = brazilTable
      .withStartRow("EXP")
      .withStopRow("IMP")
      .map(row => (
        row._1,
        "BRA", // origin country
        codeToCountry(row._2), // destination country
        getFirstDayDate(row._3, row._4), // transaction date
        row._5.toFloat, // price (only net price)
        "kg", // unit
        row._6.toFloat, // amount
        row._7, // product_category
        "" // description
      ))
      .toDF("id", "origin", "destination", "transaction_date", "price", "unit", "quantity", "product_category", "description")

    Citus.appendData(expDF)

    val impDF = brazilTable
      .withStartRow("IMP")
      .map(row => (
        row._1, // id
        codeToCountry(row._2), // origin country
        "BRA", // destination country
        getFirstDayDate(row._3, row._4), // transaction date
        row._5.toFloat, // price (only net price)
        "kg", // unit
        row._6.toFloat, // amount
        row._7, // product_category
        "" // description
      ))
      .toDF("id", "origin", "destination", "transaction_date", "price", "unit", "quantity", "product_category", "description")

    Citus.appendData(impDF)

  }

  def getFirstDayDate(year: String, month: String): Date = {
    //TODO since we only have year and month, I am putting the first day of the month as date
    new java.sql.Date(new SimpleDateFormat("yyyy-MM-dd").parse(year + "-" + month + "-01").getTime)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("BDM2")
      .config("spark.hbase.host", "victreebel.fib.upc.es")
      .config("spark.hbase.port", "27000")
      .getOrCreate()

    brazil(spark)

    spark.stop()
  }

}