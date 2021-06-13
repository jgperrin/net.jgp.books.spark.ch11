package net.jgp.books.spark.ch11.lab201_global_view_reuse;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Simple SQL select on ingested data
 * 
 * @author jgp
 */
public class SimpleSelectGlobalViewFailedReuseApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    SimpleSelectGlobalViewFailedReuseApp app =
        new SimpleSelectGlobalViewFailedReuseApp();
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

    // This will fail as it is not the same application
    Dataset<Row> smallCountries =
        spark.sql(
            "SELECT * FROM global_temp.geodata WHERE yr1980 > 1 ORDER BY 2 LIMIT 5");

    // Shows at most 10 rows from the dataframe (which is limited to 5
    // anyway)
    smallCountries.show(10, false);
  }
}
