// Databricks notebook source
//  /FileStore/tables/2015_summary.csv

//Cargar CSV
val flightData2015 = spark
.read
.option("inferSchema", "true")
.option("header", "true")
.csv("/FileStore/tables/2015_summary.csv")

// COMMAND ----------

//Tomamos 3 en formato Array
flightData2015.take(3)

// COMMAND ----------

//Ordenar por count y crear plan
flightData2015.sort("count").explain()

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "5")
flightData2015.sort("count").take(2)

// COMMAND ----------

//Comprobar numero de particiones
println(spark.conf.get("spark.sql.shuffle.partitions"))

// COMMAND ----------

//Crear vista temporal
flightData2015.createOrReplaceTempView("flight_data_2015")

// COMMAND ----------

//Consulta SQL seleccion DEST_COUNTRY_NAME y cuenta
val sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
""")

//Lo mismo sin consulta de SQL
val dataFrameWay = flightData2015
.groupBy("DEST_COUNTRY_NAME")
.count()

//Crear planes para cada DataFrame
sqlWay.explain
dataFrameWay.explain

// COMMAND ----------

//Consulta para obtener el maximo dentro de la columna count
spark.sql("SELECT max(count) from flight_data_2015").take(1)
//Lo mismo
import org.apache.spark.sql.functions.max
flightData2015.select(max("count")).take(1)

// COMMAND ----------

//Consultar 5 destinos mas frecuentes
val maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")
maxSql.collect()

// COMMAND ----------

//Lo mismo que anteriormente (maxSql)
import org.apache.spark.sql.functions.desc

flightData2015
  .groupBy("DEST_COUNTRY_NAME")
  .sum("count")
  .withColumnRenamed("sum(count)", "destination_total")
  .sort(desc("destination_total"))
  .limit(5)
  .collect()

// COMMAND ----------

//Crear plan
flightData2015
  .groupBy("DEST_COUNTRY_NAME")
  .sum("count")
  .withColumnRenamed("sum(count)", "destination_total")
  .sort(desc("destination_total"))
  .limit(5)
  .explain()
