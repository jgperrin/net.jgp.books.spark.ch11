"""
  Simple SQL select on ingested data
  @author rambabu.posa
"""
from os import path
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType,StructField,
                               StringType,DoubleType)

current_dir = path.dirname(__file__)
relative_path = "../../../../data/populationbycountry19802010millions.csv"
absolute_file_path = path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("Simple SELECT using SQL") \
    .master("local[*]") \
    .getOrCreate()

schema = StructType([
    StructField('geo',StringType(), True),
    StructField('yr1980', DoubleType(), False)
])

# Reads a CSV file with header, called books.csv, stores it in a
# dataframe
df = spark.read.format("csv") \
          .option("header", True) \
          .schema(schema) \
          .load(absolute_file_path)

df.createOrReplaceGlobalTempView("geodata")
df.printSchema()

query = """
  SELECT * FROM global_temp.geodata
  WHERE yr1980 < 1 
  ORDER BY 2 
  LIMIT 5
"""

smallCountries = spark.sql(query)

# Shows at most 10 rows from the dataframe (which is limited to 5
# anyway)
smallCountries.show(10, False)

time.sleep(600)

# Good to stop SparkSession at the end of the application
spark.stop()