# Databricks notebook source
#./bin/pyspark --packages io.delta:delta-core_2.11:0.4.0 --conf "spark.databricks.
#delta.retentionDurationCheck.enabled=false" --conf "spark.sql.extensions=io.delta.sql.
#DeltaSparkSessionExtension"

# COMMAND ----------

# Location variables
tripdelaysFilePath = "/FileStore/tables/departuredelays.csv"
pathToEventsTable = "/FileStore/tables/departureDelays.delta"
# Read flight delay data
departureDelays = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(tripdelaysFilePath)

# COMMAND ----------

# Save flight delay data into Delta Lake format
departureDelays \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .save("/FileStore/tables/departureDelays.delta")


# COMMAND ----------

# Load flight delay data in Delta Lake format
delays_delta = spark \
    .read \
    .format("delta") \
    .load("/FileStore/tables/departureDelays.delta")
# Create temporary view
delays_delta.createOrReplaceTempView("delays_delta")
# How many flights are between Seattle and San Francisco
spark.sql("select count(1) from delays_delta where origin = 'SEA' and destination = 'SFO'").show()

# COMMAND ----------

from delta.tables import *
# Convert non partitioned parquet table at path '/path/to/table'
#deltaTable = DeltaTable.convertToDelta(spark, "parquet.`/path/to/table`")
# Convert partitioned parquet table at path '/path/to/table' and partitioned by integer column named 
#'part' partitionedDeltaTable = DeltaTable.convertToDelta(spark, "parquet.`/path/to/table`", "partint")

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *
# Access the Delta Lake table
deltaTable = DeltaTable.forPath(spark, pathToEventsTable)
# Delete all on-time and early flights
deltaTable.delete("delay < 0") 
# How many flights are between Seattle and San Francisco
spark.sql("select count(1) from delays_delta where origin = 'SEA' and destination = 'SFO'").show()

# COMMAND ----------

#Update all flights originating from Detroit to now be originating from Seattle
deltaTable.update("origin = 'DTW'", { "origin": "'SEA'" } ) 
# How many flights are between Seattle and San Francisco
spark.sql("select count(1) from delays_delta where origin = 'SEA' and destination = 'SFO'").show()

# COMMAND ----------

# What flights between SEA and SFO for these date periods
spark.sql("""select * from delays_delta where origin = 'SEA' and destination = 'SFO' and date like 
'1010%' limit 10""").show()

# COMMAND ----------

items = [(1010710, 31, 590, 'SEA', 'SFO'), (1010521, 10, 590, 'SEA', 'SFO'), (1010822, 31, 590, 
'SEA', 'SFO')]
cols = ['date', 'delay', 'distance', 'origin', 'destination']
merge_table = spark.createDataFrame(items, cols)
merge_table.toPandas()

# COMMAND ----------

# Merge merge_table with flights
deltaTable.alias("flights") \
 .merge(merge_table.alias("updates"),"flights.date = updates.date") \
 .whenMatchedUpdate(set = { "delay" : "updates.delay" } ) \
 .whenNotMatchedInsertAll() \
 .execute()
# What flights between SEA and SFO for these date periods
spark.sql("""select * from delays_delta where origin = 'SEA' and destination = 'SFO' and date like 
'1010%' limit 10""").show()

# COMMAND ----------

deltaTable.history().show()

# COMMAND ----------

# Load DataFrames for each version
dfv0 = spark.read.format("delta").option("versionAsOf", 0).load("/FileStore/tables/departureDelays.delta")
dfv1 = spark.read.format("delta").option("versionAsOf", 1).load("/FileStore/tables/departureDelays.delta")
dfv2 = spark.read.format("delta").option("versionAsOf", 2).load("/FileStore/tables/departureDelays.delta")
# Calculate the SEA to SFO flight counts for each version of history
cnt0 = dfv0.where("origin = 'SEA'").where("destination = 'SFO'").count()
cnt1 = dfv1.where("origin = 'SEA'").where("destination = 'SFO'").count()
cnt2 = dfv2.where("origin = 'SEA'").where("destination = 'SFO'").count()
# Print out the value
print("SEA -> SFO Counts: Create Table: %s, Delete: %s, Update: %s" % (cnt0, cnt1, cnt2))
## Output
#SEA -> SFO Counts: Create Table: 1698, Delete: 837, Update: 986

# COMMAND ----------

# Remove all files older than 0 hours old.
deltaTable.vacuum(0)

# COMMAND ----------


