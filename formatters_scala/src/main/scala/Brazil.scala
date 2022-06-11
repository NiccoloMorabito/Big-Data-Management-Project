import it.nerdammer.spark.hbase._
import org.apache.spark.sql.SparkSession

import java.sql.Date
import java.text.SimpleDateFormat

object Brazil {

  def brazil(spark: SparkSession): Unit = {
    import spark.implicits._
    val sc = spark.sparkContext

    val codeToCountry = sc.textFile("src/main/resources/PAIS.csv") //TODO put in hdfs or hbase ?
      .map(row => row.split(";").toList)
      .filter(split => split.size > 4 && !split(0).equals("\"CO_PAIS\""))
      .map(split => (split(0).replace("\"", "").toInt, split(4))) // country code -> country name (EN)
      .collectAsMap()

    /*
    val shToCategory = sc.textFile("src/main/resources/NCM_SH.csv") //TODO put in hdfs or hbase ?
      .map(row => row.split(";").toList)
      //.filter(split => split.size > 4)
      .map(split => (split(4).toInt, split(7))) // sh4 code -> NO_SH4 (EN)
      .collectAsMap()
    //println(shToCategory)
     */

    val brazilTable = sc.hbaseTable[(String, String, String, String, String, String)]("brazil")
      .select("CO_PAIS", "CO_ANO", "CO_MES", "VL_FOB", "KG_LIQUIDO", "SH4")
      .inColumnFamily("values")

    val brazilExports = brazilTable
      .withStartRow("EXP")
      .withStopRow("IMP")

    val brazilImports = brazilTable
      .withStartRow("IMP")

    val expDF = brazilExports
      .map(row => (
        "Brazil", // origin country
        codeToCountry(row._1.toInt), // destination country
        getFirstDayDate(row._2, row._3), // transaction date
        row._4.toFloat, // price (only net price)
        "kg", // unit
        row._5.toFloat, // amount
        row._6 + "00", // product_category //TODO not correct
        "" // description
      ))
      .toDF("origin", "destination", "transaction_date", "price", "unit", "quantity", "product_category", "description")

    Citus.appendData(expDF)


    val impDF = brazilImports
      .map(row => (
        codeToCountry(row._1.toInt), // origin country
        "Brazil", // destination country
        getFirstDayDate(row._2, row._3), // transaction date
        row._4.toFloat, // price (only net price)
        "kg", // unit
        row._5.toFloat, // amount
        row._6 + "00", // product_category //TODO not correct
        "" // description
      ))
      .toDF("origin", "destination", "transaction_date", "price", "unit", "quantity", "product_category", "description")

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