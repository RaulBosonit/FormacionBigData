# Databricks notebook source
from pyspark.sql import SparkSession 
from pyspark.sql.functions import *
# Create a SparkSession
spark = (SparkSession
 .builder
 .appName("SparkSQLExampleApp")
 .getOrCreate())
# Path to data set
csv_file = "/FileStore/tables/LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
# Read and create a temporary view
# Infer schema (note that for larger files you 
# may want to specify the schema)
df = (spark.read.format("csv")
 .option("inferSchema", "true")
 .option("header", "true")
 .load(csv_file))
df.createOrReplaceTempView("us_delay_flights_tbl")

# Distancia mayor a 1000 yardas
spark.sql("""SELECT distance, origin, destination 
FROM us_delay_flights_tbl WHERE distance > 1000 
ORDER BY distance DESC""").show(10)

#IGUAL CONSULTA ANTERIOR
(df.select("distance", "origin", "destination")
 .where(col("distance") > 1000)
 .orderBy(desc("distance"))).show(10)

# delay mayor 120 y origen SFO y destino ORD
spark.sql("""SELECT date, delay, origin, destination 
FROM us_delay_flights_tbl 
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' 
ORDER by delay DESC""").show(10)

#IGUAL QUE LA ANTERIOR
(df.select("date", "delay", "origin", "destination")
  .where((col("delay") > 120) & (col("origin") == 'SFO') & (col("destination") == 'ORD'))
  .orderBy(desc("delay"))).show(10)

# CASE
spark.sql("""SELECT delay, origin, destination,
 CASE
 WHEN delay > 360 THEN 'Very Long Delays'
 WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
 WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
 WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
 WHEN delay = 0 THEN 'No Delays'
 ELSE 'Early'
 END AS Flight_Delays
 FROM us_delay_flights_tbl
 ORDER BY origin, delay DESC""").show(10)

# IGUAL QUE LA ANTERIOR
(df.select("delay", "origin", "destination",
           when(col("delay") > 360, "Very Long Delays")
          .when((col("delay") > 120) & (col("delay") < 360), "Long Delays")
          .when((col("delay") > 60) & (col("delay") < 120), "Short Delays")
          .when((col("delay") > 0) & (col("delay") < 60), "Tolerable Delays")
          .when(col("delay") == 0, "No Delays")
          .otherwise("Early").alias("Flight_Delays"))
  .orderBy("origin", desc("delay"))
  .show(10))

