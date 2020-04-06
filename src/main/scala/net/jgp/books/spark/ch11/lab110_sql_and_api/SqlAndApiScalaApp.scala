package net.jgp.books.spark.ch11.lab110_sql_and_api

import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.types.{DataTypes, StructField}

/**
  * Simple SQL select on ingested data after preparing the data with the
  * dataframe API.
  *
  * @author rambabu.posa
  */
object SqlAndApiScalaApp {

  /**
    * main() is your entry point to the application.
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    // Create a session on a local master
    val spark = SparkSession.builder
      .appName("Simple SQL")
      .master("local")
      .getOrCreate

    // Create the schema for the whole dataset
    val schema = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("geo", DataTypes.StringType, true),
      DataTypes.createStructField("yr1980", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1981", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1982", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1983", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1984", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1985", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1986", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1987", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1988", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1989", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1990", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1991", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1992", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1993", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1994", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1995", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1996", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1997", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1998", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1999", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2000", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2001", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2002", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2003", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2004", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2005", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2006", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2007", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2008", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2009", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2010", DataTypes.DoubleType, false)
    ))

    // Reads a CSV file with header (as specified in the schema), called
    // populationbycountry19802010millions.csv, stores it in a dataframe
    var df = spark.read.format("csv")
      .option("header", true)
      .schema(schema)
      .load("data/populationbycountry19802010millions.csv")

    for(i <- Range(1981, 2010)) {
      df = df.drop(df.col("yr" + i))
    }

    // Creates a new column with the evolution of the population between
    // 1980
    // and 2010
    df = df.withColumn("evolution", F.expr("round((yr2010 - yr1980) * 1000000)"))
    df.createOrReplaceTempView("geodata")

    val query1 =
      """
        |SELECT * FROM geodata
        |WHERE geo IS NOT NULL AND evolution <= 0
        |ORDER BY evolution
        |LIMIT 25
      """.stripMargin
    val negativeEvolutionDf = spark.sql(query1)

    // Shows at most 15 rows from the dataframe
    negativeEvolutionDf.show(15, false)

    val query2 =
      """
        |SELECT * FROM geodata
        |WHERE geo IS NOT NULL AND evolution > 999999
        |ORDER BY evolution DESC
        |LIMIT 25
      """.stripMargin

    val moreThanAMillionDf = spark.sql(query2)
    moreThanAMillionDf.show(15, false)

    // Good to stop SparkSession at the end of the application
    spark.stop()

  }

}
