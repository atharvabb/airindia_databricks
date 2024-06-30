// Databricks notebook source
// MAGIC
// MAGIC %run "/Workspace/Users/atharvaxa9@gmail.com/utilities/Custom_SparkSession_Create"

// COMMAND ----------

// MAGIC %run "/Workspace/Users/atharvaxa9@gmail.com/utilities/Generic_DF_Reader"

// COMMAND ----------

import org.apache.spark.sql.functions._
val airport_cancel_code_schema = StructType(List(
    StructField("Code", StringType, true),
    StructField("Description", StringType, true)
))

val DFOptions = Some(Map("header"->true,"mergeSchema"->true))

val curr_date = java.time.LocalDate.now
//val curr_date ="2023-01-01"
val sink_part_col = "dw_load_date"
val airport_source_mnt_path = "/mnt/7x4/bronze/Airport"
val airport_sink_mnt_path = "/mnt/7x4/silver/Airport"
val cancel_code_source_mnt_path = "/mnt/7x4/bronze/Cancellation"
val cancel_code_sink_mnt_path = "/mnt/7x4/silver/Cancellation"

//Spark Session Creation
val spark = Spark_Session_Create("local[*]","AirIndia_Cleaning",Some(Map("spark.sql.autoBroadcastJoinThreshold"->"100")))



// COMMAND ----------

val airport_baseDF = generic_df_reader(spark,"csv",airport_source_mnt_path,"STRUCTTYPE",structSchema=airport_cancel_code_schema,extra_configs=DFOptions).filter(col("dw_load_date")===lit(curr_date))


val cancel_code_baseDF = generic_df_reader(spark,"parquet",cancel_code_source_mnt_path,"STRUCTTYPE",structSchema=airport_cancel_code_schema).filter(col("dw_load_date")===lit(curr_date))




// COMMAND ----------

val airport_trans = airport_baseDF.withColumn("City",split(col("Description"),",")(0))
                            .withColumn("Country",trim(split(split(col("Description"),":")(0),",")(1)))
                            .withColumn("Airport_Name",trim(split(col("Description"),":")(1)))

airport_trans.write.format("delta").option("mergeSchema",true).mode(SaveMode.Overwrite)
                                   .partitionBy("dw_load_date")
                                   .save(airport_sink_mnt_path) 
//airport_trans.show()
//"Agra, India: Agra Airport"

// COMMAND ----------

cancel_code_baseDF.withColumn("Code",regexp_replace(col("Code"),"[\"]",""))
                  .withColumn("Description",regexp_replace(col("Description"),"[\"]",""))
                  .write.format("delta")
                  .mode(SaveMode.Overwrite)
                  .partitionBy("dw_load_date")
                  .option("mergeSchema",true)
                  .save(cancel_code_sink_mnt_path)
