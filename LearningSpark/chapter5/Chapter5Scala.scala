// Databricks notebook source
import org.apache.spark.sql.functions._
// Set file paths
val delaysPath =
 "/FileStore/tables/LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
val airportsPath =
 "/FileStore/tables/LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/airport_codes_na.txt"
// Obtain airports data set
val airports = spark.read
 .option("header", "true")
 .option("inferschema", "true")
 .option("delimiter", "\t")
 .csv(airportsPath)
airports.createOrReplaceTempView("airports_na")
// Obtain departure Delays data set
val delays = spark.read
 .option("header","true")
 .csv(delaysPath)
 .withColumn("delay", expr("CAST(delay as INT) as delay"))
 .withColumn("distance", expr("CAST(distance as INT) as distance"))
delays.createOrReplaceTempView("departureDelays")
// Create temporary small table
val foo = delays.filter(
 expr("""origin == 'SEA' AND destination == 'SFO' AND 
 date like '01010%' AND delay > 0"""))
foo.createOrReplaceTempView("foo")

spark.sql("SELECT * FROM airports_na LIMIT 10").show()

spark.sql("SELECT * FROM departureDelays LIMIT 10").show()

spark.sql("SELECT * FROM foo").show()

// Union two tables
val bar = delays.union(foo)
bar.createOrReplaceTempView("bar")
bar.filter(expr("""origin == 'SEA' AND destination == 'SFO'
AND date LIKE '01010%' AND delay > 0""")).show()


//INNER JOIN
foo.join(
 airports.as('air),
 $"air.IATA" === $"origin"
).select("City", "State", "date", "delay", "distance", "destination").show()

//ADDING NEW COLUMS
import org.apache.spark.sql.functions.expr
val foo2 = foo.withColumn(
 "status",
 expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
 )

//DROPPING COLUMS
val foo3 = foo2.drop("delay")
foo3.show()

//RENAMING COLUMS
val foo4 = foo3.withColumnRenamed("status", "flight_status")
foo4.show()

