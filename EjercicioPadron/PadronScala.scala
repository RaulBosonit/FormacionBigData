// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*La siguiente sección de la práctica se abordará si ya se tienen suficientes conocimientos de 
Spark, en concreto de el manejo de DataFrames, y el manejo de tablas de Hive a través de 
Spark.sql.
• 6.1)
Comenzamos realizando la misma práctica que hicimos en Hive en Spark, importando el 
csv. Sería recomendable intentarlo con opciones que quiten las "" de los campos, que 
ignoren los espacios innecesarios en los campos, que sustituyan los valores vacíos por 0 y 
que infiera el esquema.
• 6.2)
De manera alternativa también se puede importar el csv con menos tratamiento en la 
importación y hacer todas las modificaciones para alcanzar el mismo estado de limpieza de
los datos con funciones de Spark.*/

//Crear SparkSession
val spark = SparkSession.builder()
  .appName("Padron")
  .getOrCreate()

//Cargar datos del CSV - 6.2
val padronDF = spark.read
  .option("header", "true")
  .option("delimiter", ";")
  .option("quote", "\"")
  .option("inferSchema", "true")
  .csv("/FileStore/tables/estadisticas202212.csv")

val padronSE = padronDF.withColumn("DESC_DISTRITO", trim(col("DESC_DISTRITO")))
                       .withColumn("DESC_BARRIO", trim(col("DESC_BARRIO")))

padronSE.show(true)
//display(padronDF)

// COMMAND ----------

//Seleccionar diferentes barrios - 6.3
val uniqueBarrios = padronDF.select("DESC_BARRIO").distinct()
uniqueBarrios.show()

// COMMAND ----------

/*• 6.4)
Crea una vista temporal de nombre "padron" y a través de ella cuenta el número de barrios
diferentes que hay.*/

//Crear vista temporal - 6.4
padronSE.createOrReplaceTempView("padron")
//Presentar el numero de barrios diferentes
val numBarrios = spark.sql("SELECT COUNT(DISTINCT DESC_BARRIO) AS num_barrios FROM padron").show()

// COMMAND ----------

/*• 6.5)
Crea una nueva columna que muestre la longitud de los campos de la columna 
DESC_DISTRITO y que se llame "longitud".*/

//Añadir Columna con longitud
val padronLG = padronSE.withColumn("LONGITUD_DISTRITO", length(col("DESC_DISTRITO")))
padronLG.show()

// COMMAND ----------

/*• 6.6)
Crea una nueva columna que muestre el valor 5 para cada uno de los registros de la tabla.*/

//Crea una nueva columna que muestre el valor 5 para cada uno de los registros de la tabla. 
val padron5 = padronLG.withColumn("Valor5", lit(5))
padron5.show()

// COMMAND ----------

/*• 6.7)
Borra esta columna.*/

val borrarColumna = padron5.drop("Valor5")
borrarColumna.show(true)

// COMMAND ----------

/*• 6.8)
Particiona el DataFrame por las variables DESC_DISTRITO y DESC_BARRIO.*/

val particionDF = padronSE.repartition(col("DESC_DISTRITO"), col("DESC_BARRIO"))
particionDF.show(true)

// COMMAND ----------

/*• 6.9)
Almacénalo en caché. Consulta en el puerto 4040 (UI de Spark) de tu usuario local el estado
de los rdds almacenados.*/

particionDF.cache()

// COMMAND ----------

/*• 6.10)
Lanza una consulta contra el DF resultante en la que muestre el número total de 
"espanoleshombres", "espanolesmujeres", extranjeroshombres" y "extranjerosmujeres" 
para cada barrio de cada distrito. Las columnas distrito y barrio deben ser las primeras en 
aparecer en el show. Los resultados deben estar ordenados en orden de más a menos 
según la columna "extran*/
//val numBarrios = spark.sql("SELECT COUNT(DISTINCT DESC_BARRIO) AS num_barrios FROM padron").show()

val numTot = padronSE.groupBy("DESC_DISTRITO", "DESC_BARRIO")
                     .agg(sum(col("ESPANOLESHOMBRES")).alias("ESPANOLESHOMBRES"),
                          sum(col("ESPANOLESMUJERES")).alias("ESPANOLESMUJERES"),
                          sum(col("EXTRANJEROSHOMBRES")).alias("EXTRANJEROSHOMBRES"),
                          sum(col("EXTRANJEROSMUJERES")).alias("EXTRANJEROSMUJERES"))
                     .orderBy(desc("EXTRANJEROSMUJERES"), desc("EXTRANJEROSHOMBRES"))

numTot.show(40)




// COMMAND ----------

/*• 6.11)
Elimina el registro en caché.*/

particionDF.unpersist()
spark.catalog.clearCache()

// COMMAND ----------

/*• 6.12)
Crea un nuevo DataFrame a partir del original que muestre únicamente una columna con 
DESC_BARRIO, otra con DESC_DISTRITO y otra con el número total de "espanoleshombres" 
residentes en cada distrito de cada barrio. Únelo (con un join) con el DataFrame original a 
través de las columnas en común.*/

val espanolesDF = padronSE.groupBy("DESC_DISTRITO", "DESC_BARRIO")
                          .agg(sum("ESPANOLESHOMBRES").alias("ESPANOLESHOMBRES"))

espanolesDF.show(40)

val espanolesJOIN = padronDF.join(espanolesDF, Seq("DESC_DISTRITO", "DESC_BARRIO"), "inner")

espanolesJOIN.show(40)

// COMMAND ----------

/*• 6.14)
Mediante una función Pivot muestra una tabla (que va a ser una tabla de contingencia) que
contenga los valores totales ()la suma de valores) de espanolesmujeres para cada distrito y 
en cada rango de edad (COD_EDAD_INT). Los distritos incluidos deben ser únicamente 
CENTRO, BARAJAS y RETIRO y deben figurar como columnas . El aspecto debe ser similar a 
este:*/

/*val prr = padronSE.select("COD_EDAD_INT").distinct().orderBy("COD_EDAD_INT")
prr.show(80)*/

val distritos = Seq("CENTRO", "BARAJAS", "RETIRO")

val pivotDF = padronSE.filter(col("DESC_DISTRITO").isin(distritos:_*))
                      .groupBy("COD_EDAD_INT")
                      .pivot("DESC_DISTRITO")
                      .agg(sum("ESPANOLESMUJERES"))
                      .orderBy("COD_EDAD_INT")

pivotDF.show()



// COMMAND ----------

/*• 6.15)
Utilizando este nuevo DF, crea 3 columnas nuevas que hagan referencia a qué porcentaje 
de la suma de "espanolesmujeres" en los tres distritos para cada rango de edad representa 
cada uno de los tres distritos. Debe estar redondeada a 2 decimales. Puedes imponerte la 
condición extra de no apoyarte en ninguna columna auxiliar creada para el caso*/




// COMMAND ----------

/*• 6.16)
Guarda el archivo csv original particionado por distrito y por barrio (en ese orden) en un 
directorio local. Consulta el directorio para ver la estructura de los ficheros y comprueba 
que es la esperada.*/

particionDF.write
    .option("header", "true")
    .option("delimiter", ";")
    .option("quote", "\"")
    .mode("overwrite")
    .csv("/FileStore/tables/padroncsv")

// COMMAND ----------

/*• 6.17)
Haz el mismo guardado pero en formato parquet. Compara el peso del archivo con el 
resultado anterior.*/

particionDF.write
    .parquet("/FileStore/tables/padronparquet")
