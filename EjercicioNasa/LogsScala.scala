// Databricks notebook source
val loadlogs = spark.read.text("/FileStore/tables/access_log_Aug95")

// COMMAND ----------

///FileStore/tables/access_log_Aug95
import org.apache.spark.sql.functions.regexp_extract
import org.apache.spark.sql.functions._

//Creamos una expresion regular para cada campo
val host = """(\S+)"""
val useridentifier = """\S+"""
val userid = """\S+"""
val date = """\[(\d{2})/(\w{3})/(\d{4}):(\d{2}:\d{2}:\d{2}) \S+\]"""
val requestmethod = """\"(\S+)"""
val resource = """\S+\s+(\S+)\s+\S+\""""
val protocol = """(HTTP/\d\.\d)\""""
val httpstatuscode = """(\d{3})"""
val size = """(\d+|-)"""

//En logsf se almacenarán los logs extraidos
val logsf = loadlogs.select(
  regexp_extract($"value", host, 1).alias("host"),
  regexp_extract($"value", useridentifier, 0).alias("useridentifier"),
  regexp_extract($"value", userid, 0).alias("userid"),
  regexp_extract($"value", date, 0).alias("date"),
  regexp_extract($"value", requestmethod, 1).alias("requestmethod"),
  regexp_extract($"value", resource, 1).alias("resource"),
  regexp_extract($"value", protocol, 1).alias("protocol"),
  regexp_extract($"value", httpstatuscode, 1).cast("integer").alias("httpstatuscode"),
  regexp_extract($"value", size, 1).cast("integer").alias("size")
)


// COMMAND ----------

//Presentar logs extraidos
display(logsf)

// COMMAND ----------

//¿Cuáles son los distintos protocolos web utilizados? Agrúpalos

//Cuenta el numero de veces que aparecen los protocolos
val protocolscount = logsf.groupBy("protocol").count().show()
//Presenta solamente los protocolos
val protocols = logsf.select("protocol").distinct().show()
//Tambien se puede hacer con spark.sql
//Primero creamos una vista temporal
logsf.createOrReplaceTempView("logs")
//Ahora lanzamos la query
val protocolssql = spark.sql("SELECT DISTINCT protocol, COUNT(*) as count FROM logs GROUP BY protocol ORDER BY count DESC").show()

// COMMAND ----------

//¿Cuáles son los códigos de estado más comunes en la web? Agrúpalos y ordénalos 

//Utilizando la vista temporal
val statuscode = spark.sql("SELECT httpstatuscode, COUNT(*) as count from logs GROUP BY httpstatuscode ORDER BY count DESC").show()

//Utilizando el df
val statuscode2 = logsf.groupBy("httpstatuscode").count().orderBy(desc("count")).show()

// COMMAND ----------

//¿Y los métodos de petición (verbos) más utilizados?

//Utilizando la vista temporal
val requestmethod = spark.sql("SELECT requestmethod, COUNT(*) as count from logs GROUP BY requestmethod ORDER BY count DESC").show()

//Utilizando el df
val requestmethod2 = logsf.groupBy("requestmethod").count().orderBy(desc("count")).show()

// COMMAND ----------

//- ¿Qué recurso tuvo la mayor transferencia de bytes de la página web?

//valor maximo
val bytes = spark.sql("SELECT resource, MAX(size) as bytes from logs GROUP BY resource ORDER BY bytes DESC").show()
//suma de size para cada resource
val resourceSize = logsf.groupBy("resource").agg(sum("size").alias("total_size")).orderBy(desc("total_size")).show()


// COMMAND ----------

//Además, queremos saber que recurso de nuestra web es el que más tráfico recibe. Es decir, el recurso con más registros en nuestro log

//Utilizando la vista temporal
val resources = spark.sql("SELECT resource, COUNT(*) as count from logs GROUP BY resource ORDER BY count DESC").show()

//Utilizando el df
val resources2 = logsf.groupBy("resource").count().orderBy(desc("count")).show()

// COMMAND ----------

import org.apache.spark.sql.functions._

//Transforma el DF en date, sustituyendo los corchetes iniciales y finales
val logsfWithDate = logsf.withColumn("date", to_date(regexp_replace($"date", "\\[|\\]", ""), "dd/MMM/yyyy:HH:mm:ss Z"))
//Muestra el trafico por fecha
val traficday = logsfWithDate.groupBy($"date").agg(sum($"size").alias("trafic")).orderBy($"trafic".desc).show()
//Presenta solo el dia
val trafficByDay = logsfWithDate.groupBy(dayofmonth($"date").alias("day")).sum("size").orderBy(desc("sum(size)")).show()




// COMMAND ----------

//- ¿Cuáles son los hosts son los más frecuentes?


//Utilizando la vista temporal
val hosts = spark.sql("SELECT host, COUNT(*) as count from logs GROUP BY host ORDER BY count DESC").show()

//Utilizando el df
val hosts2 = logsf.groupBy("host").count().orderBy(desc("count")).show()

// COMMAND ----------

//- ¿A qué horas se produce el mayor número de tráfico en la web?

//Transformar a hora
val logsfWithHour = logsfWithDate.withColumn("hour", hour($"date"))
val trafficByHour = logsfWithHour.groupBy("hour").count().orderBy(desc("count")).show
//val pr = logsfWithHour.groupBy($"hour").count().show() -> Solo esta hour 0

// COMMAND ----------

//- ¿Cuál es el número de errores 404 que ha habido cada día?

val errors404 = logsfWithDate.filter($"httpstatuscode" === "404")
val errors404ByDay = errors404.groupBy($"date").count().orderBy(desc("count")).show()

