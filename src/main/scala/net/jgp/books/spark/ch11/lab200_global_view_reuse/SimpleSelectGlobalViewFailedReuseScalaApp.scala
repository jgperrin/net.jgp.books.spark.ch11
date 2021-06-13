package net.jgp.books.spark.ch11.lab201_global_view_reuse

import org.apache.spark.sql.SparkSession

/**
  * Simple SQL select on ingested data
  *
  * @author rambabu.posa
  */
object SimpleSelectGlobalViewFailedReuseScalaApp {

  /**
    * main() is your entry point to the application.
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    // Creates a session on a local master
    val spark = SparkSession.builder
      .appName("Simple SELECT using SQL")
      .master("local[*]")
      .getOrCreate

    val query =
      """
        |SELECT * FROM global_temp.geodata
        |WHERE yr1980 > 1
        |ORDER BY 2
        |LIMIT 5
      """.stripMargin


    // This will fail as it is not the same application
    val smallCountries = spark.sql(query)

    // Shows at most 10 rows from the dataframe (which is limited to 5
    // anyway)
    smallCountries.show(10, false)

    spark.stop
  }

}
