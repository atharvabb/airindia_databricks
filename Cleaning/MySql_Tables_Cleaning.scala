// Databricks notebook source
// MAGIC %run "/Workspace/Users/atharvaxa9@gmail.com/utilities/Custom_SparkSession_Create"

// COMMAND ----------

// MAGIC %run "/Workspace/Users/atharvaxa9@gmail.com/utilities/Generic_DF_Reader"

// COMMAND ----------

import org.apache.spark.sql.functions._ 
val unique_carrier_schema = "Code string,Description string"
val curr_date = java.time.LocalDate.now
val spark = Spark_Session_Create("local[*]","AirIndia_Cleaning",Some(Map("spark.sql.autoBroadcastJoinThreshold"->"100")))


// COMMAND ----------

val unique_carrier_baseDF = generic_df_reader(spark,path="/mnt/7x4/bronze/UNIQUE_CARRIERS",schemaType="ddlstrinG",stringSchema=unique_carrier_schema,source_format="parquet").filter(col("dw_load_date")===lit(curr_date))

val cleaned_carrier_DF = unique_carrier_baseDF
                   .withColumn("Code",regexp_replace(col("Code"),"[\"]",""))
                   .withColumn("Description",regexp_replace(col("Description"),"[\"]",""))

// COMMAND ----------

cleaned_carrier_DF.write.format("delta").mode(SaveMode.Append)
                               .option("mergeSchema",true)
                               .save("/mnt/7x4/silver/UNIQUE_CARRIERS")
