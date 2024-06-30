// Databricks notebook source
// MAGIC %run "/Workspace/Users/atharvaxa9@gmail.com/utilities/Custom_SparkSession_Create"

// COMMAND ----------

// MAGIC %run "/Workspace/Users/atharvaxa9@gmail.com/utilities/Generic_DF_Reader"

// COMMAND ----------

//response
//Congifs for plane data cleaning
import org.apache.spark.sql.functions._
//val citySchema = "id int,name string,countrycode string,district string,population long"

//val cityDFOptions = Some(Map("header"->false,"mergeSchema"->true))

val curr_date = java.time.LocalDate.now
//val sink_part_col = "dw_load_date"
val source_mnt_path = "/mnt/7x4/bronze/Airplane_Api_Data"
val sink_mnt_path = "/mnt/7x4/silver/Airplane_Api_Data"

//Spark Session Creation
val spark = Spark_Session_Create("local[*]","AirIndia_Cleaning",Some(Map("spark.sql.autoBroadcastJoinThreshold"->"100")))
160


// COMMAND ----------

spark.read.format("json").option("path",source_mnt_path).load()
                         .select(explode(col("response")).alias("resp"),col("dw_load_date"))
                         .select(expr("coalesce(resp.iata_code,'NA')").alias("iata_code"),
                                 expr("coalesce(resp.icao_code,'NA')").alias("icao_code"),col("resp.name").alias("name"),col("dw_load_date")
                         ).write
                         .mode(SaveMode.Overwrite)
                         .option("mergeSchema",true)
                         .option("path",sink_mnt_path)
                         .save
  
