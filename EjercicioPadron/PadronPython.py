# Databricks notebook source
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


'''La siguiente sección de la práctica se abordará si ya se tienen suficientes conocimientos de 
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
los datos con funciones de Spark.'''


spark = (SparkSession.builder.appName("PythonPadron").getOrCreate())
padron_file = "/FileStore/tables/estadisticas202212.csv"

padron_df = (spark.read.format("csv")
                 .option("header", "true")
                 .option("delimiter", ";")
                 .option("inferSchema","true")
                 .load(padron_file))

padron_df.show()

padronSE = padron_df.withColumn("DESC_DISTRITO", trim("DESC_DISTRITO")).withColumn("DESC_BARRIO", trim("DESC_BARRIO"))

padronSE.show()

# COMMAND ----------

''' 6.3)
Enumera todos los barrios diferentes.'''

uniqueBarrios = padron_df.select("DESC_BARRIO").distinct()
uniqueBarrios.show()

# COMMAND ----------

''' 6.4)
Crea una vista temporal de nombre "padron" y a través de ella cuenta el número de barrios
diferentes que hay.'''

padronSE.createOrReplaceTempView("padron")
#Presentar el numero de barrios diferentes
numBarrios = spark.sql("SELECT COUNT(DISTINCT DESC_BARRIO) AS num_barrios FROM padron").show()

# COMMAND ----------

''' 6.5)
Crea una nueva columna que muestre la longitud de los campos de la columna 
DESC_DISTRITO y que se llame "longitud".'''

padronLG = padronSE.withColumn("LONGITUD_DISTRITO", length("DESC_DISTRITO"))
padronLG.show()

# COMMAND ----------

''' 6.6)
Crea una nueva columna que muestre el valor 5 para cada uno de los registros de la tabla.'''

padron5 = padronLG.withColumn("Valor5", lit(5))
padron5.show()

# COMMAND ----------

''' 6.7)
Borra esta columna.
'''

borrarColumna = padron5.drop("Valor5")
borrarColumna.show()

# COMMAND ----------

'''6.8)
Particiona el DataFrame por las variables DESC_DISTRITO y DESC_BARRIO.'''

particionDF = padronSE.repartition("DESC_DISTRITO", "DESC_BARRIO")
particionDF.show()

# COMMAND ----------

''' 6.9)
Almacénalo en caché. Consulta en el puerto 4040 (UI de Spark) de tu usuario local el estado
de los rdds almacenados.'''

particionDF.cache()

# COMMAND ----------

''' 6.10)
Lanza una consulta contra el DF resultante en la que muestre el número total de 
"espanoleshombres", "espanolesmujeres", extranjeroshombres" y "extranjerosmujeres" 
para cada barrio de cada distrito. Las columnas distrito y barrio deben ser las primeras en 
aparecer en el show. Los resultados deben estar ordenados en orden de más a menos 
según la columna "extranjerosmujeres" y desempatarán por la columna 
"extranjeroshombres"'''

numTot = padronSE.groupBy("DESC_DISTRITO", "DESC_BARRIO").agg(sum("ESPANOLESHOMBRES").alias("ESPANOLESHOMBRES"),sum("ESPANOLESMUJERES").alias("ESPANOLESMUJERES"),sum("EXTRANJEROSHOMBRES").alias("EXTRANJEROSHOMBRES"),sum("EXTRANJEROSMUJERES").alias("EXTRANJEROSMUJERES")).orderBy(desc("EXTRANJEROSMUJERES"), desc("EXTRANJEROSHOMBRES"))

numTot.show(40)

# COMMAND ----------

'''6.11)
Elimina el registro en caché'''

particionDF.unpersist()
spark.catalog.clearCache()

# COMMAND ----------

'''6.12)
Crea un nuevo DataFrame a partir del original que muestre únicamente una columna con 
DESC_BARRIO, otra con DESC_DISTRITO y otra con el número total de "espanoleshombres" 
residentes en cada distrito de cada barrio. Únelo (con un join) con el DataFrame original a 
través de las columnas en común.
'''

espanolesDF = padronSE.groupBy("DESC_DISTRITO", "DESC_BARRIO") \
                      .agg(sum("ESPANOLESHOMBRES").alias("ESPANOLESHOMBRES"))

espanolesDF.show(40)

espanolesJOIN = padron_df.join(espanolesDF, ["DESC_DISTRITO", "DESC_BARRIO"], "inner")

espanolesJOIN.show(40)

# COMMAND ----------

'''6.13)
Repite la función anterior utilizando funciones de ventana. (over(Window.partitionBy.....)).'''

distritos = ["CENTRO", "BARAJAS", "RETIRO"]

pivotDF = (padronSE
           .filter(col("DESC_DISTRITO").isin(distritos))
           .groupBy("COD_EDAD_INT")
           .pivot("DESC_DISTRITO")
           .agg(sum("ESPANOLESMUJERES"))
           .orderBy("COD_EDAD_INT"))

pivotDF.show()


# COMMAND ----------

'''6.15)
Utilizando este nuevo DF, crea 3 columnas nuevas que hagan referencia a qué porcentaje 
de la suma de "espanolesmujeres" en los tres distritos para cada rango de edad representa 
cada uno de los tres distritos. Debe estar redondeada a 2 decimales. Puedes imponerte la 
condición extra de no apoyarte en ninguna columna auxiliar creada para el caso'''

from pyspark.sql.window import Window

# sumamos mujeres españolas por edad y distrito
total_espanolas = pivotDF.select('COD_EDAD_INT', sum('CENTRO').over(Window.partitionBy('COD_EDAD_INT')).alias('SUM_CENTRO'),
                                          sum('BARAJAS').over(Window.partitionBy('COD_EDAD_INT')).alias('SUM_BARAJAS'),
                                          sum('RETIRO').over(Window.partitionBy('COD_EDAD_INT')).alias('SUM_RETIRO'),
                                          sum(concat('CENTRO', 'BARAJAS', 'RETIRO')).over(Window.partitionBy('COD_EDAD_INT')).alias('SUM_TOTAL'))

# calculamos el porcentaje de cada distrito respecto al total
porcentaje_centro = (total_espanolas['SUM_CENTRO'] / total_espanolas['SUM_TOTAL']).alias('PORCENTAJE_CENTRO')
porcentaje_barajas = (total_espanolas['SUM_BARAJAS'] / total_espanolas['SUM_TOTAL']).alias('PORCENTAJE_BARAJAS')
porcentaje_retiro = (total_espanolas['SUM_RETIRO'] / total_espanolas['SUM_TOTAL']).alias('PORCENTAJE_RETIRO')

# agregamos las columnas de porcentaje al DF
resultDF = total_espanolas.select('COD_EDAD_INT', porcentaje_centro, porcentaje_barajas, porcentaje_retiro)
resultDF.show()



# COMMAND ----------

'''6.16)
Guarda el archivo csv original particionado por distrito y por barrio (en ese orden) en un 
directorio local. Consulta el directorio para ver la estructura de los ficheros y comprueba 
que es la esperada.'''

particionDF.write \
    .option("header", "true") \
    .option("delimiter", ";") \
    .option("quote", "\"") \
    .mode("overwrite") \
    .csv("/FileStore/tables/padroncsvpy")

# COMMAND ----------

''' 6.17)
Haz el mismo guardado pero en formato parquet. Compara el peso del archivo con el 
resultado anterior.'''

particionDF.write \
    .mode("overwrite") \
    .parquet("/FileStore/tables/padronparquetpy")
