"""
Simple SQL select on ingested data
@author rambabu.posa
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType,StructField,
                               StringType,DoubleType)
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/populationbycountry19802010millions.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("Simple SELECT using SQL") \
    .master("local[*]") \
    .getOrCreate()

schema = StructType([
    StructField('geo',StringType(), True),
    StructField('yr1980', DoubleType(), False)
  ])

# Reads a CSV file with header, called books.csv, stores it in a dataframe
df = spark.read.csv(header=True, inferSchema=True, schema=schema, path=absolute_file_path)

df.createOrReplaceTempView('geodata')
df.printSchema()

query = """
  SELECT * FROM geodata
  WHERE yr1980 < 1
  ORDER BY 2
  LIMIT 5
"""

smallCountries = spark.sql(query)

# Shows at most 10 rows from the dataframe (which is limited to 5
# anyway)
smallCountries.show(10, False)

# Good to stop SparkSession at the end of the application
spark.stop()