package net.jgp.books.spark.ch11.lab201_global_view_reuse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField}

/**
  * Simple SQL select on ingested data
  *
  * @author rambabu.posa
  */
object SimpleSelectGlobalViewScalaApp {

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

    val schema = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("geo", DataTypes.StringType, true),
      DataTypes.createStructField("yr1980", DataTypes.DoubleType, false)
    ))

    // Reads a CSV file with header, called books.csv, stores it in a
    // dataframe
    val df = spark.read.format("csv")
      .option("header", true)
      .schema(schema)
      .load("data/populationbycountry19802010millions.csv")

    df.createOrReplaceGlobalTempView("geodata")
    df.printSchema()

    val query =
      """
        |SELECT * FROM global_temp.geodata
        |WHERE yr1980 < 1
        |ORDER BY 2
        |LIMIT 5
      """.stripMargin

    val smallCountries = spark.sql(query)

    // Shows at most 10 rows from the dataframe (which is limited to 5
    // anyway)
    smallCountries.show(10, false)

    try
      Thread.sleep(60000)
    catch {
      case e: InterruptedException =>
        e.printStackTrace()
    }

    spark.stop
  }

}
