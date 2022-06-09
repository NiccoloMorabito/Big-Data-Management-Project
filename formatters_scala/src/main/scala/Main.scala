import Citus.{CITUS_JDBC_URL, CITUS_PROPERTIES}
import it.nerdammer.spark.hbase._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Date
import java.text.SimpleDateFormat

object Main {

  val BRAZIL_COLUMNS = "CO_ANO;\"CO_MES\";\"CO_NCM\";\"CO_UNID\";\"CO_PAIS\";\"SG_UF_NCM\";\"CO_VIA\";\"CO_URF\";\"QT_ESTAT\";\"KG_LIQUIDO\";\"VL_FOB\""
  val DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");

  def brazil(spark: SparkSession): Unit = {
    import spark.implicits._
    val sc = spark.sparkContext

    val codeToCountry = sc.textFile("src/main/resources/PAIS.csv") //TODO put in hdfs or hbase
      .map(row => row.split(";").toList)
      .filter(split => split.size > 4)
      .map(split => (split(0), split(4))) // country code -> country name (EN)
      .collectAsMap()
    println(codeToCountry)

    val brazilTable = sc.hbaseTable[(String, String, String, Int, Int)]("brazil")
      .select("CO_ANO", "CO_MES", "CO_PAIS", "KG_LIQUIDO", "VL_FOB")
      .inColumnFamily("values")

    val brazilExports = brazilTable
      .withStartRow("EXP")
      .withStopRow("IMP")

    val brazilImports = brazilTable
      .withStartRow("IMP")


    val impDF = brazilImports
      .map(row => (
        codeToCountry(row._3), // origin country
        "Brazil", // destination country
        getFirstDayDate(row._1, row._2), // transaction date
        row._5.toFloat, // price (only net price)
        "kg", // unit
        row._4, // amount
        "", // product_category TODO
        "" // description TODO
      ))
      .toDF("origin", "destination", "transaction_date", "price", "unit", "quantity", "product_category", "description")

    Citus.appendData(impDF)


    val expDF = brazilExports
      .map(row => (
        "Brazil", // origin country
        codeToCountry(row._3), // destination country
        getFirstDayDate(row._1, row._2), // transaction date
        row._5.toFloat, // price (only net price)
        "kg", // unit
        row._4, // amount
        "", // product_category TODO
        "" // description TODO
      ))
      .toDF("origin", "destination", "transaction_date", "price", "unit", "quantity", "product_category", "description")

    Citus.appendData(expDF)
  }

  def getFirstDayDate(year: String, month: String): Date = {
    //TODO since we only have year and month, I am putting the first day of the month as date
    new java.sql.Date(DATE_FORMATTER.parse(year + "-" + month + "-01").getTime)
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