// Databricks notebook source
// /FileStore/tables/retail-data/by-day/2010_12_01.csv

//Cargar datos
val staticDataFrame = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/FileStore/tables/retail-data/by-day/*.csv")
//Crear vista temporal
staticDataFrame.createOrReplaceTempView("retail_data")
//Esquema
val staticSchema = staticDataFrame.schema

// COMMAND ----------

import org.apache.spark.sql.functions.{window, column, desc, col}


staticDataFrame
  .selectExpr(
  "CustomerId",
  "(UnitPrice * Quantity) as total_cost",
  "InvoiceDate")
  .groupBy(
  col("CustomerId"), window(col("InvoiceDate"), "1 day"))
  .sum("total_cost")
  .show(5)

// COMMAND ----------

//Establecer limite de particiones a 5
spark.conf.set("spark.sql.shuffle.partitions", "5")

// COMMAND ----------

//Leer utilizando readStream en lugar de read
val streamingDataFrame = spark.readStream
  .schema(staticSchema)
  .option("maxFilesPerTrigger", 1)
  .format("csv")
  .option("header", "true")
  .load("/FileStore/tables/retail-data/by-day/*.csv")

// COMMAND ----------

streamingDataFrame.isStreaming // returns true si esta funcionando correctamente

// COMMAND ----------

//Vamos a realizar la misma consulta que anteriormente
val purchaseByCustomerPerHour = streamingDataFrame
  .selectExpr(
  "CustomerId",
  "(UnitPrice * Quantity) as total_cost",
  "InvoiceDate")
  .groupBy(
  $"CustomerId", window($"InvoiceDate", "1 day"))
  .sum("total_cost")

// COMMAND ----------

purchaseByCustomerPerHour.writeStream
  .format("memory") // memory = store in-memory table
  .queryName("customer_purchases") // counts = name of the in-memory table
  .outputMode("complete") // complete = all the counts should be in the table
  .start()

// COMMAND ----------

spark.sql("""
SELECT *
FROM customer_purchases
ORDER BY `sum(total_cost)` DESC
""")
.show(5)

// COMMAND ----------

staticDataFrame.printSchema()

// COMMAND ----------

import org.apache.spark.sql.functions.date_format

//Transformando los datos en numericos
val preppedDataFrame = staticDataFrame
  .na.fill(0)
  .withColumn("day_of_week", date_format($"InvoiceDate", "EEEE"))
  .coalesce(5)

// COMMAND ----------

//Splitear datos en funcion de la fecha
val trainDataFrame = preppedDataFrame
.where("InvoiceDate < '2011-07-01'")
val testDataFrame = preppedDataFrame
.where("InvoiceDate >= '2011-07-01'")

// COMMAND ----------

trainDataFrame.count()
testDataFrame.count()

// COMMAND ----------

import org.apache.spark.ml.feature.StringIndexer

//Transformar dia de la semana en indice
val indexer = new StringIndexer()
  .setInputCol("day_of_week")
  .setOutputCol("day_of_week_index")

// COMMAND ----------

import org.apache.spark.ml.feature.OneHotEncoder

//Transformar dia de la semana en encoder
val encoder = new OneHotEncoder()
  .setInputCol("day_of_week_index")
  .setOutputCol("day_of_week_encoded")

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler

//Creando vector, ya que es lo que se solicita normalmente como entrada
val vectorAssembler = new VectorAssembler()
  .setInputCols(Array("UnitPrice", "Quantity", "day_of_week_encoded"))
  .setOutputCol("features")

// COMMAND ----------

import org.apache.spark.ml.Pipeline

//Crear pipeline o tuberia para que futuros datos pasen por este proceso de transformacion
val transformationPipeline = new Pipeline()
  .setStages(Array(indexer, encoder, vectorAssembler))

// COMMAND ----------

//Adaptar transformadores. Para ello se debe ver el numero de valores unicos que van a ser indexados
val fittedPipeline = transformationPipeline.fit(trainDataFrame)

// COMMAND ----------

//Transformando los datos
val transformedTraining = fittedPipeline.transform(trainDataFrame)

// COMMAND ----------

//Almacenar en cache
transformedTraining.cache()

// COMMAND ----------

import org.apache.spark.ml.clustering.KMeans

//Instanciando el modelo que vamos a usar
val kmeans = new KMeans()
  .setK(20)
  .setSeed(1L)

// COMMAND ----------

val kmModel = kmeans.fit(transformedTraining)

// COMMAND ----------

//Sirve para ver el coste computacional
//kmModel.computeCost(transformedTraining)
//ERROR

// COMMAND ----------

//Hace la transformacion
val transformedTest = fittedPipeline.transform(testDataFrame)

// COMMAND ----------

//Al volver a ver el coste computacional despues de haber hecho la transformacion
//este debe de haber disminuido
//kmModel.computeCost(transformedTest)

//ERROR
