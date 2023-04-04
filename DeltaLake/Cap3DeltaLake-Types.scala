// Databricks notebook source
// /FileStore/tables/retail-data/by-day/2010_12_01.csv

val df = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/FileStore/tables/retail-data/by-day/2010_12_01.csv")

df.printSchema()
df.createOrReplaceTempView("dfTable")

// COMMAND ----------

import org.apache.spark.sql.functions.lit
df.select(lit(5), lit("five"), lit(5.0))

// COMMAND ----------

//---------------------------
//--TRABAJANDO CON BOOLEANS--
//---------------------------

// COMMAND ----------

import org.apache.spark.sql.functions.col

df.where(col("InvoiceNo").equalTo(536365))
  .select("InvoiceNo", "Description")
  .show(5, false)

// COMMAND ----------

val priceFilter = col("UnitPrice") > 600

val descripFilter = col("Description").contains("POSTAGE")

df.where(col("StockCode").isin("DOT"))
  .where(priceFilter.or(descripFilter))
  .show()

// COMMAND ----------

val DOTCodeFilter = col("StockCode") === "DOT"
val priceFilter = col("UnitPrice") > 600
val descripFilter = col("Description").contains("POSTAGE")

df.withColumn("isExpensive",
  DOTCodeFilter.and(priceFilter.or(descripFilter)))
  .where("isExpensive")
  .select("unitPrice", "isExpensive")
  .show(5)

// COMMAND ----------

//EXPRESIONES EQUIVALENTES

import org.apache.spark.sql.functions.{expr, not, col}

df.withColumn("isExpensive", not(col("UnitPrice").leq(250)))
  .filter("isExpensive")
  .select("Description", "UnitPrice")
  .show(5)

df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))
  .filter("isExpensive")
  .select("Description", "UnitPrice")
  .show(5)

// COMMAND ----------

//---------------------------
//--TRABAJANDO CON NUMEROS---
//---------------------------

// COMMAND ----------

import org.apache.spark.sql.functions.{expr, pow}

val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5

df.select(
  expr("CustomerId"),
  fabricatedQuantity.alias("realQuantity"))
  .show(2)

// COMMAND ----------

//EXPRESIONES IGUALES

df.selectExpr(
  "CustomerId",
  "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity")
  .show(2)

// COMMAND ----------

import org.apache.spark.sql.functions.{round, bround}

df.select(
  round(col("UnitPrice"), 1).alias("rounded"),
  col("UnitPrice"))
  .show(5)

// COMMAND ----------

import org.apache.spark.sql.functions.lit

//round redondea hacia arriba mientras que bround hacia abajo

df.select(
  round(lit("2.5")),
  bround(lit("2.5")))
  .show(2)

// COMMAND ----------

import org.apache.spark.sql.functions.{corr}

df.stat.corr("Quantity", "UnitPrice")
df.select(corr("Quantity", "UnitPrice")).show()

// COMMAND ----------

import org.apache.spark.sql.functions._

df.select("Quantity").count()
df.agg(max(col("Quantity")).alias("Maximo"), min(col("Quantity")).alias("Minimo")).show()

// COMMAND ----------

val colName = "UnitPrice"
val quantileProbs = Array(0.5)
val relError = 0.05
df.stat.approxQuantile("UnitPrice", quantileProbs, relError) // 2.51

// COMMAND ----------

df.stat.crosstab("StockCode", "Quantity").show()
df.stat.freqItems(Seq("StockCode", "Quantity")).show()

// COMMAND ----------

import org.apache.spark.sql.functions.monotonically_increasing_id

//Crea un valor por fila creciente

df.select(monotonically_increasing_id()).show(2)

// COMMAND ----------

//---------------------------
//--TRABAJANDO CON STRINGS---
//---------------------------

// COMMAND ----------

import org.apache.spark.sql.functions.{initcap}

//Añadir mayus

df.select(initcap(col("Description"))).show(2, false)

// COMMAND ----------

import org.apache.spark.sql.functions.{lower, upper}

df.select(
  col("Description"),
  lower(col("Description")),
  upper(lower(col("Description"))))
  .show(2)

// COMMAND ----------

import org.apache.spark.sql.functions.{lit, ltrim, rtrim, rpad, lpad, trim}

//Diferentes funciones TRIM

df.select(
  ltrim(lit(" HELLO ")).as("ltrim"),
  rtrim(lit(" HELLO ")).as("rtrim"),
  trim(lit(" HELLO ")).as("trim"),
  lpad(lit("HELLO"), 3, " ").as("lp"),
  rpad(lit("HELLO"), 10, " ").as("rp"))
  .show(2)

// COMMAND ----------

//------------------------------------------
//---TRABAJANDO CON EXPRESIONES REGULARES---
//------------------------------------------

// COMMAND ----------

import org.apache.spark.sql.functions.regexp_replace

val simpleColors = Seq("black", "white", "red", "green", "blue")
val regexString = simpleColors.map(_.toUpperCase).mkString("|")
// the | signifies `OR` in regular expression syntax

df.select(
  regexp_replace(col("Description"), regexString, "COLOR")
  .alias("color_cleaned"),
  col("Description"))
  .show(2)

// COMMAND ----------

import org.apache.spark.sql.functions.translate

df.select(
  translate(col("Description"), "LEET", "1337"),
  col("Description"))
  .show(2)

// COMMAND ----------

import org.apache.spark.sql.functions.regexp_extract

val regexString = simpleColors
  .map(_.toUpperCase)
  .mkString("(", "|", ")")
// the | signifies OR in regular expression syntax

df.select(
  regexp_extract(col("Description"), regexString, 1)
  .alias("color_cleaned"),
  col("Description"))
  .show(2)

// COMMAND ----------

val containsBlack = col("Description").contains("BLACK")
val containsWhite = col("DESCRIPTION").contains("WHITE")

df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
  .filter("hasSimpleColor")
  .select("Description")
  .show(3, false)

// COMMAND ----------

val simpleColors = Seq("black", "white", "red", "green", "blue")

val selectedColumns = simpleColors.map(color => {
  col("Description")
  .contains(color.toUpperCase)
  .alias(s"is_$color")
  }):+expr("*") // could also append this value

df
  .select(selectedColumns:_*)
  .where(col("is_white").or(col("is_red")))
  .select("Description")
  .show(3, false)

// COMMAND ----------

//---------------------------
//---TRABAJANDO CON FECHAS---
//---------------------------

// COMMAND ----------

import org.apache.spark.sql.functions.{current_date, current_timestamp}

val dateDF = spark.range(10)
  .withColumn("today", current_date())
  .withColumn("now", current_timestamp())

dateDF.createOrReplaceTempView("dateTable")

dateDF.printSchema()

// COMMAND ----------

import org.apache.spark.sql.functions.{date_add, date_sub}

dateDF
  .select(
  date_sub(col("today"), 5),
  date_add(col("today"), 5))
  .show(1)

// COMMAND ----------

import org.apache.spark.sql.functions.{datediff, months_between, to_date}

dateDF
  .withColumn("week_ago", date_sub(col("today"), 7))
  .select(datediff(col("week_ago"), col("today")))
  .show(1)

dateDF
  .select(
  to_date(lit("2016-01-01")).alias("start"),
  to_date(lit("2017-05-22")).alias("end"))
  .select(months_between(col("start"), col("end")))
  .show(1)

// COMMAND ----------

//CONVERT

import org.apache.spark.sql.functions.{to_date, lit}

spark.range(5).withColumn("date", lit("2017-01-01"))
  .select(to_date(col("date")))
  .show(1)

// COMMAND ----------

//SI NO PARSEA LA FECHA DEVUELVE NULL, NO ERROR
dateDF.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(1)

// COMMAND ----------

import org.apache.spark.sql.functions.{unix_timestamp, from_unixtime}

val dateFormat = "yyyy-dd-MM"

val cleanDateDF = spark.range(1)
  .select(
    to_date(lit("2017-12-11"), dateFormat)
 .alias("date"),
   to_date(lit("2017-20-12"), dateFormat)
 .alias("date2"))

cleanDateDF.createOrReplaceTempView("dateTable2")

// COMMAND ----------

import org.apache.spark.sql.functions.to_timestamp

cleanDateDF
  .select(
    to_timestamp(col("date"), dateFormat))
  .show()

// COMMAND ----------

//--------------------------
//---TRABAJANDO CON NULLS---
//--------------------------

// COMMAND ----------

import org.apache.spark.sql.functions.coalesce

df.select(coalesce(col("Description"), col("CustomerId"))).show()

// COMMAND ----------

df.na.drop()
df.na.drop("any")

// COMMAND ----------

df.na.drop("all", Seq("StockCode", "InvoiceNo"))
df.show()

// COMMAND ----------

df.na.fill(5, Seq("StockCode", "InvoiceNo"))
df.show()

// COMMAND ----------

val fillColValues = Map(
  "StockCode" -> 5,
  "Description" -> "No Value"
)

df.na.fill(fillColValues)
df.filter(col("Description") === "No Value").show()

// COMMAND ----------

df.na.replace("Description", Map("" -> "UNKNOWN"))

// COMMAND ----------

//-----------------------------
//---TRABAJANDO CON ORDERING---
//-----------------------------

// COMMAND ----------

import org.apache.spark.sql.functions.struct

val complexDF = df
  .select(struct("Description", "InvoiceNo").alias("complex"))

complexDF.createOrReplaceTempView("complexDF")

// COMMAND ----------

complexDF.select("complex.Description").show()
complexDF.select(col("complex").getField("Description")).show()

// COMMAND ----------

//---------------------------
//---TRABAJANDO CON ARRAYS---
//---------------------------

// COMMAND ----------

import org.apache.spark.sql.functions.split

df.select(split(col("Description"), " ")).show(2)

// COMMAND ----------

df.select(split(col("Description"), " ").alias("array_col"))
  .selectExpr("array_col[1]")
  .show(2)

// COMMAND ----------

import org.apache.spark.sql.functions.size

df.select(size(split(col("Description"), " "))).show(2) // shows 5 and 3

// COMMAND ----------

import org.apache.spark.sql.functions.array_contains

df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)

// COMMAND ----------

import org.apache.spark.sql.functions.{split, explode}

df.withColumn("splitted", split(col("Description"), " "))
  .withColumn("exploded", explode(col("splitted")))
  .select("Description", "InvoiceNo", "exploded")
  .show(2)

// COMMAND ----------

import org.apache.spark.sql.functions.map

df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
  .selectExpr("complex_map['Description']")
  .show(2)

// COMMAND ----------

df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
 .selectExpr("complex_map['WHITE METAL LANTERN']")
 .show(2)

// COMMAND ----------

df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
 .selectExpr("explode(complex_map)")
 .show(2)

// COMMAND ----------

//------------------------------
//---TRABAJANDO CON FUNCIONES---
//------------------------------

// COMMAND ----------

val udfExampleDF = spark.range(5).toDF("num")
def power3(number:Double):Double = {
 number * number * number
}
power3(2.0)

// COMMAND ----------

import org.apache.spark.sql.functions.udf
val power3udf = udf(power3(_:Double):Double)
udfExampleDF.select(power3udf(col("num"))).show()

// COMMAND ----------

spark.udf.register("power3", power3(_:Double):Double)
udfExampleDF.selectExpr("power3(num)").show(10)
