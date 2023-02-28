# Databricks notebook source
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = (SparkSession.builder.appName("PythonMnMCount").getOrCreate())
mnm_file = "/FileStore/tables/LearningSparkV2-master/chapter2/py/src/data/mnm_dataset.csv"

mnm_df = (spark.read.format("csv").option("header", "true").option("inferSchema","true").load(mnm_file))

count_mnm_df = (mnm_df
 .select("State", "Color", "Count")
 .groupBy("State", "Color")
 .agg(count("Count").alias("Total"))
 .orderBy("Total", ascending=False))

count_mnm_df.show(n=60, truncate=False)
print("Total Rows = %d" % (count_mnm_df.count()))

ca_count_mnm_df = (mnm_df
 .select("State", "Color", "Count")
 .where(mnm_df.State == "CA")
 .groupBy("State", "Color")
 .agg(count("Count").alias("Total"))
 .orderBy("Total", ascending=False))
 
ca_count_mnm_df.show(n=10, truncate=False)

