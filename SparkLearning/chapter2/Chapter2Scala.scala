// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder.appName("MnMCount").getOrCreate()

val mnmFile = "/FileStore/tables/LearningSparkV2-master/chapter2/scala/data/mnm_dataset.csv"
val mnmDF = spark.read.format("csv")
  .option("header","true")
  .option("inferSchema", "true")
  .load(mnmFile)

val countMnMDF = mnmDF.select("State", "Color", "Count")
  .groupBy("State", "Color")
  .agg(count("Count").alias("Total"))
  .orderBy(desc("Total"))

countMnMDF.show(60)
println(s"Total Rows = ${countMnMDF.count()}")
println()

// COMMAND ----------


