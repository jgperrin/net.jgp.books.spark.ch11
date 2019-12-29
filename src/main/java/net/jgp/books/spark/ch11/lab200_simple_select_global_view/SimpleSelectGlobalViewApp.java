package net.jgp.books.spark.ch11.lab200_simple_select_global_view;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Simple SQL select on ingested data, using a global view
 * 
 * @author jgp
 */
public class SimpleSelectGlobalViewApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    SimpleSelectGlobalViewApp app = new SimpleSelectGlobalViewApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Simple SELECT using SQL")
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
            false) });

    // Reads a CSV file with header, called books.csv, stores it in a
    // dataframe
    Dataset<Row> df = spark.read().format("csv")
        .option("header", true)
        .schema(schema)
        .load("data/populationbycountry19802010millions.csv");
    df.createOrReplaceGlobalTempView("geodata");
    df.printSchema();

    Dataset<Row> smallCountriesDf =
        spark.sql(
            "SELECT * FROM global_temp.geodata "
                + "WHERE yr1980 < 1 ORDER BY 2 LIMIT 5");

    // Shows at most 10 rows from the dataframe (which is limited to 5
    // anyway)
    smallCountriesDf.show(10, false);

    // Create a new session and query the same data
    SparkSession spark2 = spark.newSession();
    Dataset<Row> slightlyBiggerCountriesDf =
        spark2.sql(
            "SELECT * FROM global_temp.geodata "
                + "WHERE yr1980 >= 1 ORDER BY 2 LIMIT 5");
    slightlyBiggerCountriesDf.show(10, false);
  }
}
