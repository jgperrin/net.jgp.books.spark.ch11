"""
  Simple SQL select on ingested data
  @author rambabu.posa
"""

from pyspark.sql import SparkSession

# Creates a session on a local master
spark = SparkSession.builder.appName("Simple SELECT using SQL") \
    .master("local[*]") \
    .getOrCreate()

query = """
  SELECT * FROM global_temp.geodata
  WHERE yr1980 > 1
  ORDER BY 2
  LIMIT 5
"""

# This will fail as it is not the same application
smallCountries = spark.sql(query)

# Shows at most 10 rows from the dataframe (which is limited to 5 anyway)
smallCountries.show(10, False)

# Good to stop SparkSession at the end of the application
spark.stop()