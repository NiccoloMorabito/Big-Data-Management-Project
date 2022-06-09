import org.apache.spark.sql.{DataFrame, SaveMode}

import java.util.Properties

object Citus {
  val CITUS_HOST = "clefable.fib.upc.edu"
  val CITUS_PORT = 9700
  val CITUS_USER = "postgres"
  val CITUS_PASSWORD = "postgres"
  val CITUS_DATABASE = "bdm"
  val CITUS_TABLE = "transactions"
  val CITUS_PROPERTIES = new Properties()
  CITUS_PROPERTIES.setProperty("user", CITUS_USER)
  CITUS_PROPERTIES.setProperty("password", CITUS_PASSWORD)
  CITUS_PROPERTIES.setProperty("driver", "org.postgresql.Driver")
  val CITUS_JDBC_URL = s"jdbc:postgresql://$CITUS_HOST:$CITUS_PORT/$CITUS_DATABASE"


  def appendData(data: DataFrame): Unit = {
    data.write.mode(SaveMode.Append).jdbc(CITUS_JDBC_URL, CITUS_TABLE, CITUS_PROPERTIES)
  }

  def overwriteData(data: DataFrame): Unit = {
    data.write.mode(SaveMode.Overwrite).jdbc(CITUS_JDBC_URL, CITUS_TABLE, CITUS_PROPERTIES)
  }

}