// Databricks notebook source
// PRIMER EJEMPLO

import org.apache.spark.sql.SparkSession 
//Crear SparkSession
val spark = SparkSession
 .builder
 .appName("SparkSQLExampleApp")
 .getOrCreate()
// Path to data set 
val csvFile="/FileStore/tables/LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
// Read and create a temporary view
// Infer schema (note that for larger files you may want to specify the schema)
val df = spark.read.format("csv")
 .option("inferSchema", "true")
 .option("header", "true")
 .load(csvFile)
// Create a temporary view
df.createOrReplaceTempView("us_delay_flights_tbl")
