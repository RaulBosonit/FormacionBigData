// Databricks notebook source
/*./bin/pyspark --packages io.delta:delta-core_2.11:0.4.0 --conf "spark.databricks.
delta.retentionDurationCheck.enabled=false" --conf "spark.sql.extensions=io.delta.sql.
DeltaSparkSessionExtension"*/

// COMMAND ----------

// /FileStore/tables/departuredelays.csv
//display(dbutils.fs.ls("dbfs:/FileStore/tables/departureDelays.delta"))





// COMMAND ----------

val tripdelaysFilePath = "/FileStore/tables/departuredelays.csv"
val pathToEventsTable = "/FileStore/tables/departureDelays.delta"
// Read flight delay data
val departureDelays = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(tripdelaysFilePath)

// COMMAND ----------

//guardar a delta

departureDelays
  .write
  .format("delta")
  .mode("overwrite")
  .save("/FileStore/tables/departureDelays.delta")

// COMMAND ----------

val delays_delta = spark
  .read
  .format("delta")
  .load("/FileStore/tables/departureDelays.delta")

delays_delta.createOrReplaceTempView("delays_delta")
// How many flights are between Seattle and San Francisco
spark.sql("select count(1) from delays_delta where origin = 'SEA' and destination = 'SFO'").show()

// COMMAND ----------

import io.delta.tables._
import org.apache.spark.sql.functions._

//DELETE


// Accede a la tabla de Delta Lake
val deltaTable = DeltaTable.forPath(spark, pathToEventsTable)

// Elimina todos los vuelos a tiempo y temprano
deltaTable.delete("delay < 0")

// ¿Cuántos vuelos hay entre Seattle y San Francisco?
spark.sql("select count(1) from delays_delta where origin = 'SEA' and destination = 'SFO'").show()


// COMMAND ----------

//UPDATE

// Actualiza el origen de los vuelos desde Detroit a Seattle
deltaTable.update(col("origin") === "DTW", Map("origin" -> lit("SEA")))

// ¿Cuántos vuelos hay entre Seattle y San Francisco?
spark.sql("select count(1) from delays_delta where origin = 'SEA' and destination = 'SFO'").show()


// COMMAND ----------

//What flights between SEA and SFO for these date periods
spark.sql("""select * from delays_delta where origin = 'SEA' and destination = 'SFO' and date like 
'1010%' limit 10""").show()

// COMMAND ----------


