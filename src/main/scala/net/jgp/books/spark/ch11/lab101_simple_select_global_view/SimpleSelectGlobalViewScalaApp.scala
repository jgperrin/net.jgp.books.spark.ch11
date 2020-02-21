package net.jgp.books.spark.ch11.lab101_simple_select_global_view

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField}

/**
  * Simple SQL select on ingested data, using a global view
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
      .master("local")
      .getOrCreate

    val schema = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("geo", DataTypes.StringType, true),
      DataTypes.createStructField("yr1980", DataTypes.DoubleType, false))
    )

    // Reads a CSV file with header, called books.csv, stores it in a
    // dataframe
    val df = spark.read.format("csv")
      .option("header", true)
      .schema(schema)
      .load("data/populationbycountry19802010millions.csv")

    df.createOrReplaceGlobalTempView("geodata")
    df.printSchema()

    val query1 =
      """
        |SELECT * FROM global_temp.geodata
        |WHERE yr1980 < 1
        |ORDER BY 2
        |LIMIT 5
      """.stripMargin
    val smallCountriesDf = spark.sql(query1)

    // Shows at most 10 rows from the dataframe (which is limited to 5 anyway)
    smallCountriesDf.show(10, false)

    val query2 =
      """
        |SELECT * FROM global_temp.geodata
        |WHERE yr1980 >= 1
        |ORDER BY 2
        |LIMIT 5
      """.stripMargin

    // Create a new session and query the same data
    val spark2 = spark.newSession
    val slightlyBiggerCountriesDf = spark2.sql(query2)

    slightlyBiggerCountriesDf.show(10, false)

    spark.stop
  }

}
