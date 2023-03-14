// Databricks notebook source
import org.apache.spark.sql.SparkSession

//Crear Spark Session
val spark = SparkSession.builder()
    .appName("SparkSession")
    .master("local[*]")
    .getOrCreate()

//Crear Base de Datos
val createDatabase = "CREATE DATABASE IF NOT EXISTS datos_padron"
spark.sql(createDatabase)


// COMMAND ----------

//Poner en uso la base de datos creada
val setDatabase = "USE datos_padron"
spark.sql(setDatabase)

// COMMAND ----------

//Crear tabla

val createTable = """ CREATE TABLE IF NOT EXISTS padron_txt (
  COD_DISTRITO STRING, 
  DESC_DISTRITO STRING,
  COD_DIST_BARRIO STRING, 
  DESC_BARRIO STRING, 
  COD_BARRIO STRING,
  COD_DIST_SECCION STRING, 
  COD_SECCION STRING, 
  COD_EDAD_INT STRING, 
  ESPANOLESHOMBRES STRING, 
  ESPANOLESMUJERES STRING,
  EXTRANJEROSHOMBRES STRING,
  EXTRANJEROSMUJERES STRING,
  FX_CARGA STRING, 
  FX_DATOS_INI STRING, 
  FX_DATOS_FIN STRING) 
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ';'
TBLPROPERTIES ('skip.header.line.count'='1')
STORED AS TEXTFILE 
"""

spark.sql(createTable)

// COMMAND ----------

//Cargar Datos
val loadData = """
    LOAD DATA INPATH '/FileStore/tables/estadisticas202212.csv'
    INTO TABLE padron_txt"""

spark.sql(loadData)


// COMMAND ----------

//Transformar datos NULL en 0
val createTablePQ = """ CREATE TABLE IF NOT EXISTS padron_parquet (
  COD_DISTRITO STRING, 
  DESC_DISTRITO STRING,
  COD_DIST_BARRIO STRING, 
  DESC_BARRIO STRING, 
  COD_BARRIO STRING,
  COD_DIST_SECCION STRING, 
  COD_SECCION STRING, 
  COD_EDAD_INT STRING, 
  ESPANOLESHOMBRES STRING, 
  ESPANOLESMUJERES STRING,
  EXTRANJEROSHOMBRES STRING,
  EXTRANJEROSMUJERES STRING,
  FX_CARGA STRING, 
  FX_DATOS_INI STRING, 
  FX_DATOS_FIN STRING) 
STORED AS PARQUET 
"""

val insertarPadronPQ = """
INSERT INTO padron_parquet
SELECT
  COD_DISTRITO, 
  DESC_DISTRITO,
  COD_DIST_BARRIO, 
  DESC_BARRIO, 
  COD_BARRIO,
  COD_DIST_SECCION, 
  COD_SECCION, 
  COD_EDAD_INT, 
  CASE WHEN ESPANOLESHOMBRES = "null" THEN 0 ELSE ESPANOLESHOMBRES END AS ESPANOLESHOMBRES, 
  CASE WHEN ESPANOLESMUJERES = "null" THEN 0 ELSE ESPANOLESMUJERES END AS ESPANOLESMUJERES,
  CASE WHEN EXTRANJEROSHOMBRES = "null" THEN 0 ELSE EXTRANJEROSHOMBRES END AS EXTRANJEROSHOMBRES,
  CASE WHEN EXTRANJEROSMUJERES = "null" THEN 0 ELSE EXTRANJEROSMUJERES END AS EXTRANJEROSMUJERES,
  FX_CARGA, 
  FX_DATOS_INI, 
  FX_DATOS_FIN
FROM padron_txt
"""

spark.sql(createTablePQ)
spark.sql(insertarPadronPQ)

// COMMAND ----------

//Alguna consulta

val query = """  select 
  sum(espanoleshombres) as SumEspHom, 
  sum(espanolesmujeres) as SumEspMuj, 
  sum(extranjeroshombres) as SumExtHom, 
  sum(extranjerosmujeres) as SumExtMuj, 
  DESC_DISTRITO, DESC_BARRIO
FROM padron_txt
group by DESC_DISTRITO, DESC_BARRIO """

spark.sql(query).show()
