"""
Simple SQL select on ingested data, using a global view
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

# Reads a CSV file with header, called books.csv, stores it in a
# dataframe
df = spark.read.format("csv") \
          .option("header", True) \
          .schema(schema) \
          .load(absolute_file_path)

df.createOrReplaceGlobalTempView("geodata")
df.printSchema()

query1 = """
    SELECT * FROM global_temp.geodata 
    WHERE yr1980 < 1 
    ORDER BY 2 
    LIMIT 5
"""
smallCountriesDf = spark.sql(query1)

# Shows at most 10 rows from the dataframe (which is limited to 5 anyway)
smallCountriesDf.show(10, False)

query2 = """
    SELECT * FROM global_temp.geodata
    WHERE yr1980 >= 1 
    ORDER BY 2 
    LIMIT 5
"""

# Create a new session and query the same data
spark2 = spark.newSession()
slightlyBiggerCountriesDf = spark2.sql(query2)

slightlyBiggerCountriesDf.show(10, False)

# Good to stop SparkSession at the end of the application
spark.stop()
spark2.stop()
