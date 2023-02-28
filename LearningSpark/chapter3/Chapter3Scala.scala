// Databricks notebook source
//EJEMPLO 1 - cell 1

import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.SparkSession
// Create a DataFrame using SparkSession
val spark = SparkSession
 .builder
 .appName("AuthorsAges")
 .getOrCreate()
// Create a DataFrame of names and ages
val dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25),
 ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")
// Group the same names together, aggregate their ages, and compute an average
val avgDF = dataDF.groupBy("name").agg(avg("age"))
// Show the results of the final execution
avgDF.show()

// COMMAND ----------

// EJEMPLO 2 - cell 2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

val spark = SparkSession
 .builder
 .appName("Example-3_7")
 .getOrCreate()

val jsonFile = "/FileStore/tables/LearningSparkV2-master/chapter3/scala/data/blogs.json"
 // Define our schema programmatically
val schema = StructType(Array(StructField("Id", IntegerType, false),
 StructField("First", StringType, false),
 StructField("Last", StringType, false),
 StructField("Url", StringType, false),
 StructField("Published", StringType, false),
 StructField("Hits", IntegerType, false),
 StructField("Campaigns", ArrayType(StringType), false)))
 // Create a DataFrame by reading from the JSON file 
 // with a predefined schema
val blogsDF = spark.read.schema(schema).json(jsonFile)
 // Show the DataFrame schema as output
blogsDF.show(false)
// Print the schema
println(blogsDF.printSchema)
println(blogsDF.schema)

// COMMAND ----------

//EJEMPLO 3 - cell 3

val sampleDF = spark
 .read
 .option("samplingRatio", 0.001)
 .option("header", true)
 .csv("""/FileStore/tables/LearningSparkV2-master/chapter3/data/sf_fire_calls.csv""")


// COMMAND ----------

// PAGINA 95 - cell 4

// Definimos la clase
case class DeviceIoTData (battery_level: Long, c02_level: Long,
cca2: String, cca3: String, cn: String, device_id: Long,
device_name: String, humidity: Long, ip: String, latitude: Double,
lcd: String, longitude: Double, scale:String, temp: Long,
timestamp: Long)

// Leer fichero y almacenar el Dataset
val ds = spark.read
.json("/FileStore/tables/LearningSparkV2-master/databricks-datasets/learning-spark-v2/iot-devices/iot_devices.json")
.as[DeviceIoTData]

ds.show(5)

// Filtrar
val filterTempDS = ds.filter({d => {d.temp > 30 && d.humidity > 70};})
filterTempDS.show(5, false)

// Creamos una clase
case class DeviceTempByCountry(temp: Long, device_name: String, device_id: Long,
 cca3: String)
// Filtra y almacena como la clase creada
val dsTemp = ds
 .filter(d => {d.temp > 25})
 .map(d => (d.temp, d.device_name, d.device_id, d.cca3))
 .toDF("temp", "device_name", "device_id", "cca3")
 .as[DeviceTempByCountry]
//Muestra los 5 primeros datos
dsTemp.show(5, false)

// Vamos a hacer lo mismo que dsTemp, hacemos el select, filtramos y almacenamos como la clase creada
val dsTemp2 = ds
 .select($"temp", $"device_name", $"device_id", $"device_id", $"cca3")
 .where("temp > 25")
 .as[DeviceTempByCountry]



