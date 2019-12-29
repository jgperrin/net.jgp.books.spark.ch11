package net.jgp.books.spark.ch11.lab400_delete;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dropping data using SQL
 * 
 * @author jgp
 */
public class DeleteApp {
  private static transient Logger log = LoggerFactory.getLogger(
      DeleteApp.class);

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    DeleteApp app = new DeleteApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    log.debug("-> start()");

    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Simple SQL")
        .master("local")
        .getOrCreate();

    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "geo",
            DataTypes.StringType,
            true),
        DataTypes.createStructField(
            "yr1980",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1981",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1982",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1983",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1984",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1985",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1986",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1987",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1988",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1989",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1990",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1991",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1992",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1993",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1994",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1995",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1996",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1997",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1998",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1999",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr2000",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr2001",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr2002",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr2003",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr2004",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr2005",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr2006",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr2007",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr2008",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr2009",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr2010",
            DataTypes.DoubleType,
            false) });

    // Reads a CSV file with header (as specified in the schema), called
    // populationbycountry19802010millions.csv, stores it in a dataframe
    Dataset<Row> df = spark.read().format("csv")
        .option("header", true)
        .schema(schema)
        .load("data/populationbycountry19802010millions.csv");

    // Remove the columns we do not want
    for (int i = 1981; i < 2010; i++) {
      df = df.drop(df.col("yr" + i));
    }

    // Creates a new column with the evolution of the population between
    // 1980
    // and 2010
    df = df.withColumn(
        "evolution",
        functions.expr("round((yr2010 - yr1980) * 1000000)"));
    df.createOrReplaceTempView("geodata");

    log.debug("Territories in orginal dataset: {}", df.count());
    Dataset<Row> cleanedDf =
        spark.sql(
            "select * from geodata where geo is not null "
                + "and geo != 'Africa' "
                + "and geo != 'North America' "
                + "and geo != 'World' "
                + "and geo != 'Asia & Oceania' "
                + "and geo != 'Central & South America' "
                + "and geo != 'Europe' "
                + "and geo != 'Eurasia' "
                + "and geo != 'Middle East' "
                + "order by yr2010 desc");
    log.debug("Territories in cleaned dataset: {}",
        cleanedDf.count());
    cleanedDf.show(20, false);
  }
}
