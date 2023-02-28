# Databricks notebook source
# EJEMPLO 1

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
# Create a DataFrame using SparkSession
spark = (SparkSession
 .builder
 .appName("AuthorsAges")
 .getOrCreate())
# Create a DataFrame 
data_df = spark.createDataFrame([("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)], ["name", "age"])
# Group the same names together, aggregate their ages, and compute an average
avg_df = data_df.groupBy("name").agg(avg("age"))
# Show the results of the final execution
avg_df.show()

# COMMAND ----------

#EJEMPLO 2 

from pyspark.sql.types import *
from pyspark.sql.functions import *

# Programmatic way to define a schema 
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
    StructField('UnitID', StringType(), True),
    StructField('IncidentNumber', IntegerType(), True),
    StructField('CallType', StringType(), True), 
    StructField('CallDate', StringType(), True), 
    StructField('WatchDate', StringType(), True),
    StructField('CallFinalDisposition', StringType(), True),
    StructField('AvailableDtTm', StringType(), True),
    StructField('Address', StringType(), True), 
    StructField('City', StringType(), True), 
    StructField('Zipcode', IntegerType(), True), 
    StructField('Battalion', StringType(), True), 
    StructField('StationArea', StringType(), True), 
    StructField('Box', StringType(), True), 
    StructField('OriginalPriority', StringType(), True), 
    StructField('Priority', StringType(), True), 
    StructField('FinalPriority', IntegerType(), True), 
    StructField('ALSUnit', BooleanType(), True), 
    StructField('CallTypeGroup', StringType(), True),
    StructField('NumAlarms', IntegerType(), True),
    StructField('UnitType', StringType(), True),
    StructField('UnitSequenceInCallDispatch', IntegerType(), True),
    StructField('FirePreventionDistrict', StringType(), True),
    StructField('SupervisorDistrict', StringType(), True),
    StructField('Neighborhood', StringType(), True),
    StructField('Location', StringType(), True),
    StructField('RowID', StringType(), True),
    StructField('Delay', FloatType(), True)])
# Use the DataFrameReader interface to read a CSV file
sf_fire_file = "/FileStore/tables/LearningSparkV2-master/chapter3/data/sf_fire_calls.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

#Selecciona los Numeros de incidente, fecha y tipo de llamada de la tabla, mientras que la columna no sea medical incident
few_fire_df = (fire_df
 .select("IncidentNumber", "AvailableDtTm", "CallType")
 .where(col("CallType") != "Medical Incident"))
few_fire_df.show(5, truncate=False)

# Suma los tipos de llamadas distintas que ha habido
(fire_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .agg(countDistinct("CallType").alias("DistinctCallTypes"))
 .show())

# Muestra 10 tipos de llamadas diferentes
(fire_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .distinct()
 .show(10, False))

# Crea un parquet file
#parquet_path = "/FileStore/tables/pq"
#fire_df.write.format("parquet").save(parquet_path)

# Modifica el nombre de la columna "Delay" y muestra aquelos mayores que 5
new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
(new_fire_df
 .select("ResponseDelayedinMins")
 .where(col("ResponseDelayedinMins") > 5)
 .show(5, False))

# Casteando columnas de tipo string a tipo Date
fire_ts_df = (new_fire_df
 .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
 .drop("CallDate")
 .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
 .drop("WatchDate")
 .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
 "MM/dd/yyyy hh:mm:ss a"))
 .drop("AvailableDtTm"))
# Select the converted columns
(fire_ts_df
 .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
 .show(5, False))

# Filtrando por año, ya que al haber casteado podemos usar la función year...
(fire_ts_df
 .select(year('IncidentDate'))
 .distinct()
 .orderBy(year('IncidentDate'))
 .show())

# Viendo cuales fueron las llamadas mas comunes
(fire_ts_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .groupBy("CallType")
 .count()
 .orderBy("count", ascending=False)
 .show(n=10, truncate=False))

# Vamos a usar funciones max, min, sum y avg

import pyspark.sql.functions as F
(fire_ts_df
 .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
 F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
 .show())








