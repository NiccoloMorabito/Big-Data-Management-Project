import it.nerdammer.spark.hbase._
import org.apache.spark.sql.{SparkSession}

object Main {

  val BRAZIL_COLUMNS = "CO_ANO;\"CO_MES\";\"CO_NCM\";\"CO_UNID\";\"CO_PAIS\";\"SG_UF_NCM\";\"CO_VIA\";\"CO_URF\";\"QT_ESTAT\";\"KG_LIQUIDO\";\"VL_FOB\""

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("BDM2")
      .config("spark.hbase.host", "victreebel.fib.upc.es")
      .config("spark.hbase.port", "27000")
      .getOrCreate()

    val sc = spark.sparkContext

    val codeToCountry = sc.textFile("src/main/resources/PAIS.csv") //TODO put in hdfs or hbase
      .map( row => row.split(";").toList )
      .filter( split => split.size > 4)
      .map( split => (split(0), split(4)) ) // country code -> country name (EN)
      .collectAsMap()

    println(codeToCountry)

    // hbase table
    val brazilTable = sc.hbaseTable[(String, String)]("brazilTable")
      .select(BRAZIL_COLUMNS)
      .inColumnFamily("values")

    val rows = brazilTable.map( r => r._2.split(";").toList)
    rows.map( row => (
        row(0), //year
        row(1).replace("\"", "").toInt, //month
        codeToCountry.get(row(4)).get, //importing/destination country
        row(9), // amount
        row(10))) //price (only net price)
      .foreach(println)

    spark.stop()
  }
}