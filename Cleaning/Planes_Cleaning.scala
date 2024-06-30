// Databricks notebook source
// MAGIC %run "/Workspace/Users/atharvaxa9@gmail.com/utilities/Custom_SparkSession_Create"

// COMMAND ----------

// MAGIC %run "/Workspace/Users/atharvaxa9@gmail.com/utilities/Generic_DF_Reader"

// COMMAND ----------

//Congifs for plane data cleaning
import org.apache.spark.sql.functions._
val PlaneSchema = "tailnum string,type string,manufacturer string,issue_date string,model string,status string,aircraft_type string,engine_type string,year int"

val PlaneDFOptions = Some(Map("header"->false,"mergeSchema"->true))

val curr_date = java.time.LocalDate.now



// COMMAND ----------

//Spark Session Creation
val spark = Spark_Session_Create("local[*]","AirIndia_Cleaning",Some(Map("spark.sql.autoBroadcastJoinThreshold"->"100")))

// COMMAND ----------

val planes_baseDF = generic_df_reader(spark,"csv","/mnt/7x4/bronze/Plane","ddlstrinG",stringSchema=PlaneSchema,extra_configs=PlaneDFOptions).filter(col("dw_load_date")===lit(curr_date))

// COMMAND ----------

val cleanedDF = planes_baseDF.withColumn("manufacturer",coalesce(col("manufacturer"),lit("Unknown")))
                             .withColumn("issue_date",coalesce(col("issue_date"),lit("9999-12-31")))
                             .withColumn("model",coalesce(col("model"),lit("Unknown")))
                             .withColumn("status",coalesce(col("aircraft_type"),lit("Unknown")))
                             .withColumn("engine_type",coalesce(col("engine_type"),lit("Unknown")))
                             .filter(!col("tailnum").isin("#Classification:"))
                             .drop("year")

// COMMAND ----------

cleanedDF.write.format("delta").mode(SaveMode.Append)
                               .option("mergeSchema",true)
                               .save("/mnt/7x4/silver/Plane")
