# Databricks notebook source
# Import pandas
import pandas as pd
# Import various pyspark SQL functions including pandas_udf
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType
# Declare the cubed function 
def cubed(a: pd.Series) -> pd.Series:
 return a * a * a
# Create the pandas UDF for the cubed function 
cubed_udf = pandas_udf(cubed, returnType=LongType())
# Create a Pandas Series
x = pd.Series([1, 2, 3])
# The function for a pandas_udf executed with local Pandas data
print(cubed(x))

# Create a Spark DataFrame, 'spark' is an existing SparkSession
df = spark.range(1, 4)
# Execute function as a Spark vectorized UDF
df.select("id", cubed_udf(col("id"))).show()


# COMMAND ----------

# Set file paths
from pyspark.sql.functions import expr
tripdelaysFilePath = "/FileStore/tables/LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
airportsnaFilePath = "/FileStore/tables/LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/airport_codes_na.txt"
#
# Obtain airports data set
airportsna = (spark.read
 .format("csv")
 .options(header="true", inferSchema="true", sep="\t")
 .load(airportsnaFilePath))
airportsna.createOrReplaceTempView("airports_na")
# Obtain departure delays data set
departureDelays = (spark.read
 .format("csv")
 .options(header="true")
 .load(tripdelaysFilePath))
departureDelays = (departureDelays
 .withColumn("delay", expr("CAST(delay as INT) as delay"))
 .withColumn("distance", expr("CAST(distance as INT) as distance")))
departureDelays.createOrReplaceTempView("departureDelays")
# Create temporary small table
foo = (departureDelays
 .filter(expr("""origin == 'SEA' and destination == 'SFO' and 
 date like '01010%' and delay > 0""")))
foo.createOrReplaceTempView("foo")

spark.sql("SELECT * FROM airports_na LIMIT 10").show()

spark.sql("SELECT * FROM departureDelays LIMIT 10").show()

spark.sql("SELECT * FROM foo").show()

# UNION
# Union two tables
bar = departureDelays.union(foo)
bar.createOrReplaceTempView("bar")
# Show the union (filtering for SEA and SFO in a specific time range)
bar.filter(expr("""origin == 'SEA' AND destination == 'SFO'
AND date LIKE '01010%' AND delay > 0""")).show()

#ADDING COLUMS
from pyspark.sql.functions import expr
foo2 = (foo.withColumn(
 "status",
 expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
 ))

# DROPPING COLUMS
foo3 = foo2.drop("delay")
foo3.show()

# RENAMING COLUMS
foo4 = foo3.withColumnRenamed("status", "flight_status")
foo4.show()
